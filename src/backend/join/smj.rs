//! Sort-Merge Join executor with external sort and merge-join.

use std::fs::{self, OpenOptions};
use std::io::{self};

use crate::catalog::types::{Catalog, Column};
use crate::disk::create_page;
use crate::heap::insert_tuple;
use crate::page::PAGE_SIZE;
use crate::table::page_count;

use super::JoinType;
use super::condition::{JoinCondition, evaluate_conditions};
use super::scanner::TupleScanner;
use super::result::JoinResult;
use super::tuple::{ColumnValue, Tuple};

/// A sorted run file on disk.
pub struct SortRun {
    pub file_path: String,
    pub page_count: u32,
}

/// Sort-Merge Join executor.
pub struct SMJExecutor {
    pub left_table: String,
    pub right_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
    pub memory_pages: usize,
}

impl SMJExecutor {
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        // Ensure tmp directory exists
        fs::create_dir_all("database/tmp")?;

        // Get schemas
        let left_database = catalog.databases.get(db).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Database not found")
        })?;
        let left_schema = left_database.tables.get(&self.left_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Left table not found"))?
            .columns.clone();
        let right_schema = left_database.tables.get(&self.right_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Right table not found"))?
            .columns.clone();

        // Get the sort key column names from the first condition
        let left_sort_col = self.conditions.first()
            .map(|c| c.left_col.clone())
            .unwrap_or_default();
        let right_sort_col = self.conditions.first()
            .map(|c| c.right_col.clone())
            .unwrap_or_default();

        // Sort both relations
        let left_runs = self.sort_relation(&self.left_table, db, catalog, &left_sort_col)?;
        let right_runs = self.sort_relation(&self.right_table, db, catalog, &right_sort_col)?;

        // Merge runs for each side
        let left_merged = self.merge_all_runs(left_runs, &left_schema)?;
        let right_merged = self.merge_all_runs(right_runs, &right_schema)?;

        // Perform merge join
        let result = self.merge_join(&left_merged, &right_merged, &left_schema, &right_schema, &left_sort_col, &right_sort_col)?;

        // Cleanup temp files
        let _ = self.cleanup_temp_files();

        Ok(result)
    }

    fn sort_relation(&self, table: &str, db: &str, catalog: &Catalog, sort_col: &str) -> io::Result<Vec<SortRun>> {
        let mut scanner = TupleScanner::new(db, table, catalog)?;
        let schema = scanner.schema.clone();
        let mut runs = Vec::new();
        let mut run_id = 0u32;

        loop {
            // Read a batch of tuples (simulating memory_pages worth)
            let mut batch: Vec<Tuple> = Vec::new();
            let batch_limit = self.memory_pages * 100; // approximate tuples per memory buffer

            for _ in 0..batch_limit {
                match scanner.next_tuple() {
                    Some(t) => batch.push(t),
                    None => break,
                }
            }

            if batch.is_empty() {
                break;
            }

            // Sort batch by sort column
            batch.sort_by(|a, b| {
                let va = a.get_field(sort_col);
                let vb = b.get_field(sort_col);
                match (va, vb) {
                    (Some(va), Some(vb)) => {
                        va.partial_cmp_values(vb).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            });

            // Write sorted batch to temp file
            let run_path = format!("database/tmp/sort_run_{}_{}.tmp", table, run_id);
            let mut run_file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open(&run_path)?;

            // Write header
            let mut header = vec![0u8; PAGE_SIZE];
            header[0..4].copy_from_slice(&1u32.to_le_bytes());
            run_file.write_all(&header)?;
            use std::io::Write;
            run_file.flush()?;

            // Create first data page
            create_page(&mut run_file)?;

            // Insert sorted tuples
            for t in &batch {
                let bytes = self.serialize_tuple(t, &schema);
                insert_tuple(&mut run_file, &bytes)?;
            }

            let pc = page_count(&mut run_file)?;
            runs.push(SortRun {
                file_path: run_path,
                page_count: pc,
            });
            run_id += 1;
        }

        Ok(runs)
    }

    fn merge_all_runs(&self, runs: Vec<SortRun>, schema: &[Column]) -> io::Result<SortRun> {
        if runs.is_empty() {
            // Create an empty run
            let path = "database/tmp/empty_run.tmp".to_string();
            let mut file = OpenOptions::new()
                .create(true).write(true).read(true).truncate(true)
                .open(&path)?;
            let mut header = vec![0u8; PAGE_SIZE];
            header[0..4].copy_from_slice(&1u32.to_le_bytes());
            use std::io::Write;
            file.write_all(&header)?;
            file.flush()?;
            create_page(&mut file)?;
            let pc = page_count(&mut file)?;
            return Ok(SortRun { file_path: path, page_count: pc });
        }

        if runs.len() == 1 {
            return Ok(runs.into_iter().next().unwrap());
        }

        // Collect all tuples from all runs, sort, write to single run
        let sort_col = self.conditions.first()
            .map(|c| c.left_col.clone())
            .unwrap_or_default();

        let mut all_tuples: Vec<Tuple> = Vec::new();
        for run in &runs {
            let file = OpenOptions::new().read(true).write(true).open(&run.file_path)?;
            let mut scanner = TupleScanner::from_file(file, schema.to_vec())?;
            all_tuples.extend(scanner.collect_all());
        }

        all_tuples.sort_by(|a, b| {
            let va = a.get_field(&sort_col);
            let vb = b.get_field(&sort_col);
            match (va, vb) {
                (Some(va), Some(vb)) => {
                    va.partial_cmp_values(vb).unwrap_or(std::cmp::Ordering::Equal)
                }
                _ => std::cmp::Ordering::Equal,
            }
        });

        let merged_path = format!("database/tmp/merged_{}.tmp", uuid_simple());
        let mut merged_file = OpenOptions::new()
            .create(true).write(true).read(true).truncate(true)
            .open(&merged_path)?;

        let mut header = vec![0u8; PAGE_SIZE];
        header[0..4].copy_from_slice(&1u32.to_le_bytes());
        use std::io::Write;
        merged_file.write_all(&header)?;
        merged_file.flush()?;
        create_page(&mut merged_file)?;

        for t in &all_tuples {
            let bytes = self.serialize_tuple(t, schema);
            insert_tuple(&mut merged_file, &bytes)?;
        }

        let pc = page_count(&mut merged_file)?;
        Ok(SortRun { file_path: merged_path, page_count: pc })
    }

    fn merge_join(
        &self,
        left_run: &SortRun,
        right_run: &SortRun,
        left_schema: &[Column],
        right_schema: &[Column],
        left_sort_col: &str,
        right_sort_col: &str,
    ) -> io::Result<JoinResult> {
        let left_file = OpenOptions::new().read(true).write(true).open(&left_run.file_path)?;
        let right_file = OpenOptions::new().read(true).write(true).open(&right_run.file_path)?;

        let mut left_scanner = TupleScanner::from_file(left_file, left_schema.to_vec())?;
        let mut right_scanner = TupleScanner::from_file(right_file, right_schema.to_vec())?;

        let mut result = JoinResult::new(left_schema, right_schema, &self.left_table, &self.right_table);

        // Collect all sorted tuples for merge
        let left_tuples = left_scanner.collect_all();
        let right_tuples = right_scanner.collect_all();

        let mut li = 0usize;
        let mut ri = 0usize;

        match self.join_type {
            JoinType::Inner | JoinType::Cross => {
                while li < left_tuples.len() && ri < right_tuples.len() {
                    let lv = left_tuples[li].get_field(left_sort_col);
                    let rv = right_tuples[ri].get_field(right_sort_col);

                    match (lv, rv) {
                        (Some(lv), Some(rv)) => {
                            match lv.partial_cmp_values(rv) {
                                Some(std::cmp::Ordering::Less) => { li += 1; }
                                Some(std::cmp::Ordering::Greater) => { ri += 1; }
                                Some(std::cmp::Ordering::Equal) => {
                                    // Collect all right tuples with same key
                                    let mut right_group = Vec::new();
                                    let key_val = rv.clone();
                                    let mut rj = ri;
                                    while rj < right_tuples.len() {
                                        if let Some(rv2) = right_tuples[rj].get_field(right_sort_col) {
                                            if rv2.eq_value(&key_val) {
                                                right_group.push(&right_tuples[rj]);
                                                rj += 1;
                                            } else {
                                                break;
                                            }
                                        } else {
                                            break;
                                        }
                                    }

                                    // Match all left tuples with same key against right group
                                    while li < left_tuples.len() {
                                        if let Some(lv2) = left_tuples[li].get_field(left_sort_col) {
                                            if lv2.eq_value(&key_val) {
                                                for r in &right_group {
                                                    if evaluate_conditions(&self.conditions, &left_tuples[li], r) {
                                                        result.add(Tuple::merge(&left_tuples[li], r));
                                                    }
                                                }
                                                li += 1;
                                            } else {
                                                break;
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                    ri = rj;
                                }
                                None => { li += 1; }
                            }
                        }
                        _ => { li += 1; }
                    }
                }
            }
            JoinType::LeftOuter => {
                // For left outer, all left tuples must appear
                let right_idx = 0usize;
                for l in &left_tuples {
                    let mut matched = false;
                    // Find matching right tuples
                    let mut rj = right_idx;
                    while rj < right_tuples.len() {
                        let lv = l.get_field(left_sort_col);
                        let rv = right_tuples[rj].get_field(right_sort_col);
                        match (lv, rv) {
                            (Some(lv), Some(rv)) => {
                                match lv.partial_cmp_values(rv) {
                                    Some(std::cmp::Ordering::Greater) => { rj += 1; continue; }
                                    Some(std::cmp::Ordering::Equal) => {
                                        if evaluate_conditions(&self.conditions, l, &right_tuples[rj]) {
                                            result.add(Tuple::merge(l, &right_tuples[rj]));
                                            matched = true;
                                        }
                                        rj += 1;
                                    }
                                    _ => break,
                                }
                            }
                            _ => break,
                        }
                    }
                    if !matched {
                        let null_right = Tuple::null_tuple(right_schema);
                        result.add(Tuple::merge(l, &null_right));
                    }
                }
            }
            _ => {
                // Fallback: use NLJ-style for other join types on sorted data
                for l in &left_tuples {
                    for r in &right_tuples {
                        if evaluate_conditions(&self.conditions, l, r) {
                            result.add(Tuple::merge(l, r));
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Serialize a Tuple back to raw bytes (INT=4 bytes LE, TEXT=10 bytes fixed).
    fn serialize_tuple(&self, tuple: &Tuple, schema: &[Column]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (i, col) in schema.iter().enumerate() {
            if let Some(val) = tuple.values.get(i) {
                match col.data_type.as_str() {
                    "INT" => {
                        match val {
                            ColumnValue::Int(v) => bytes.extend_from_slice(&v.to_le_bytes()),
                            _ => bytes.extend_from_slice(&0i32.to_le_bytes()),
                        }
                    }
                    "TEXT" => {
                        match val {
                            ColumnValue::Text(s) => {
                                let mut text_bytes = s.as_bytes().to_vec();
                                if text_bytes.len() > 10 {
                                    text_bytes.truncate(10);
                                } else if text_bytes.len() < 10 {
                                    text_bytes.extend(vec![b' '; 10 - text_bytes.len()]);
                                }
                                bytes.extend_from_slice(&text_bytes);
                            }
                            _ => bytes.extend_from_slice(&[b' '; 10]),
                        }
                    }
                    _ => {}
                }
            }
        }
        bytes
    }

    fn cleanup_temp_files(&self) -> io::Result<()> {
        let tmp_dir = "database/tmp";
        if let Ok(entries) = fs::read_dir(tmp_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
        Ok(())
    }
}

/// Simple pseudo-UUID for unique temp file names.
fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos)
}