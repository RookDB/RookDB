//! Hash Join executor (Simple in-memory + Grace Hash Join).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};

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

/// In-memory hash table for build phase.
pub struct HashTable {
    pub buckets: HashMap<u64, Vec<Tuple>>,
}

/// Hash Join executor.
pub struct HashJoinExecutor {
    pub build_table: String,
    pub probe_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
    pub memory_pages: usize,
    pub num_partitions: usize,
}

impl HashJoinExecutor {
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        // Determine if build table fits in memory
        let build_path = format!("database/base/{}/{}.dat", db, self.build_table);
        let mut build_file = OpenOptions::new().read(true).write(true).open(&build_path)?;
        let build_pages = page_count(&mut build_file)?;

        if build_pages as usize <= self.memory_pages {
            self.execute_simple(db, catalog)
        } else {
            self.execute_grace(db, catalog)
        }
    }

    fn execute_simple(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut build_scanner = TupleScanner::new(db, &self.build_table, catalog)?;
        let mut probe_scanner = TupleScanner::new(db, &self.probe_table, catalog)?;

        let build_schema = build_scanner.schema.clone();
        let probe_schema = probe_scanner.schema.clone();

        // Determine which column to hash on
        let build_col = self.conditions.first()
            .map(|c| {
                // The build table might be left or right in the condition
                if c.left_table == self.build_table {
                    c.left_col.clone()
                } else {
                    c.right_col.clone()
                }
            })
            .unwrap_or_default();

        let probe_col = self.conditions.first()
            .map(|c| {
                if c.left_table == self.probe_table {
                    c.left_col.clone()
                } else {
                    c.right_col.clone()
                }
            })
            .unwrap_or_default();

        // Build phase
        let ht = self.build_phase(&mut build_scanner, &build_col);

        // Probe phase
        let result = self.probe_phase(&ht, &mut probe_scanner, &probe_col, &build_schema, &probe_schema);

        Ok(result)
    }

    fn build_phase(&self, scanner: &mut TupleScanner, hash_col: &str) -> HashTable {
        let mut buckets: HashMap<u64, Vec<Tuple>> = HashMap::new();

        while let Some(t) = scanner.next_tuple() {
            let key = self.hash_value(t.get_field(hash_col));
            buckets.entry(key).or_insert_with(Vec::new).push(t);
        }

        HashTable { buckets }
    }

    fn probe_phase(
        &self,
        ht: &HashTable,
        scanner: &mut TupleScanner,
        hash_col: &str,
        build_schema: &[Column],
        probe_schema: &[Column],
    ) -> JoinResult {
        // Determine output order: build is "left" in condition, probe is "right"
        // But we need to match the output schema to left_table/right_table from conditions
        let (left_table, right_table, left_schema, right_schema, build_is_left) = {
            if let Some(c) = self.conditions.first() {
                if c.left_table == self.build_table {
                    (self.build_table.clone(), self.probe_table.clone(), build_schema, probe_schema, true)
                } else {
                    (self.probe_table.clone(), self.build_table.clone(), probe_schema, build_schema, false)
                }
            } else {
                (self.build_table.clone(), self.probe_table.clone(), build_schema, probe_schema, true)
            }
        };

        let mut result = JoinResult::new(left_schema, right_schema, &left_table, &right_table);

        while let Some(p) = scanner.next_tuple() {
            let key = self.hash_value(p.get_field(hash_col));

            if let Some(build_tuples) = ht.buckets.get(&key) {
                for b in build_tuples {
                    // Evaluate conditions with proper left/right orientation
                    let (left, right) = if build_is_left { (b, &p) } else { (&p, b) };
                    if evaluate_conditions(&self.conditions, left, right) {
                        result.add(Tuple::merge(left, right));
                    }
                }
            }
        }

        result
    }

    fn execute_grace(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        fs::create_dir_all("database/tmp")?;

        let database = catalog.databases.get(db).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Database not found")
        })?;

        let build_schema = database.tables.get(&self.build_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Build table not found"))?
            .columns.clone();
        let probe_schema = database.tables.get(&self.probe_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Probe table not found"))?
            .columns.clone();

        let build_col = self.conditions.first()
            .map(|c| {
                if c.left_table == self.build_table { c.left_col.clone() } else { c.right_col.clone() }
            })
            .unwrap_or_default();
        let probe_col = self.conditions.first()
            .map(|c| {
                if c.left_table == self.probe_table { c.left_col.clone() } else { c.right_col.clone() }
            })
            .unwrap_or_default();

        let build_is_left = self.conditions.first()
            .map(|c| c.left_table == self.build_table)
            .unwrap_or(true);

        let (left_table, right_table, left_schema, right_schema) = if build_is_left {
            (self.build_table.clone(), self.probe_table.clone(), build_schema.clone(), probe_schema.clone())
        } else {
            (self.probe_table.clone(), self.build_table.clone(), probe_schema.clone(), build_schema.clone())
        };

        let num_parts = self.num_partitions.max(2);

        // Partition build table
        self.partition_table(db, catalog, &self.build_table, &build_col, &build_schema, "build", num_parts)?;
        // Partition probe table
        self.partition_table(db, catalog, &self.probe_table, &probe_col, &probe_schema, "probe", num_parts)?;

        let mut result = JoinResult::new(&left_schema, &right_schema, &left_table, &right_table);

        // For each partition pair, do simple hash join
        for i in 0..num_parts {
            let build_part_path = format!("database/tmp/hash_part_build_{}.tmp", i);
            let probe_part_path = format!("database/tmp/hash_part_probe_{}.tmp", i);

            if !std::path::Path::new(&build_part_path).exists() || !std::path::Path::new(&probe_part_path).exists() {
                continue;
            }

            let build_file = OpenOptions::new().read(true).write(true).open(&build_part_path)?;
            let probe_file = OpenOptions::new().read(true).write(true).open(&probe_part_path)?;

            let mut build_scanner = TupleScanner::from_file(build_file, build_schema.clone())?;
            let mut probe_scanner = TupleScanner::from_file(probe_file, probe_schema.clone())?;

            // Build in-memory hash table from build partition
            let ht = self.build_phase(&mut build_scanner, &build_col);

            // Probe
            while let Some(p) = probe_scanner.next_tuple() {
                let key = self.hash_value(p.get_field(&probe_col));
                if let Some(build_tuples) = ht.buckets.get(&key) {
                    for b in build_tuples {
                        let (left, right) = if build_is_left { (b, &p) } else { (&p, b) };
                        if evaluate_conditions(&self.conditions, left, right) {
                            result.add(Tuple::merge(left, right));
                        }
                    }
                }
            }
        }

        // Cleanup
        self.cleanup_temp_files()?;

        Ok(result)
    }

    fn partition_table(
        &self,
        db: &str,
        catalog: &Catalog,
        table: &str,
        hash_col: &str,
        schema: &[Column],
        label: &str,
        num_parts: usize,
    ) -> io::Result<()> {
        let mut scanner = TupleScanner::new(db, table, catalog)?;

        // Create partition files
        let mut part_files: Vec<File> = Vec::new();
        for i in 0..num_parts {
            let path = format!("database/tmp/hash_part_{}_{}.tmp", label, i);
            let mut f = OpenOptions::new()
                .create(true).write(true).read(true).truncate(true)
                .open(&path)?;

            // Write header
            let mut header = vec![0u8; PAGE_SIZE];
            header[0..4].copy_from_slice(&1u32.to_le_bytes());
            f.write_all(&header)?;
            f.flush()?;
            create_page(&mut f)?;
            part_files.push(f);
        }

        // Distribute tuples
        while let Some(t) = scanner.next_tuple() {
            let key = self.hash_value(t.get_field(hash_col));
            let part_idx = (key as usize) % num_parts;
            let bytes = self.serialize_tuple(&t, schema);
            insert_tuple(&mut part_files[part_idx], &bytes)?;
        }

        Ok(())
    }

    fn hash_value(&self, val: Option<&ColumnValue>) -> u64 {
        match val {
            Some(ColumnValue::Int(v)) => *v as u64,
            Some(ColumnValue::Text(s)) => {
                // Simple hash for text
                let mut hash: u64 = 5381;
                for b in s.trim().bytes() {
                    hash = hash.wrapping_mul(33).wrapping_add(b as u64);
                }
                hash
            }
            _ => 0,
        }
    }

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
                                if text_bytes.len() > 10 { text_bytes.truncate(10); }
                                else if text_bytes.len() < 10 { text_bytes.extend(vec![b' '; 10 - text_bytes.len()]); }
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
