//! Hash Join executor (in-memory + Grace partitioned).
//!
//! - Small build table → in-memory hash join.
//! - Large build table → Grace hash join with disk-based partitioning.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};

use crate::catalog::types::{Catalog, Column};
use crate::disk::create_page;
use crate::heap::insert_tuple;
use crate::page::PAGE_SIZE;
use crate::table::page_count;

use super::condition::{JoinCondition, evaluate_conditions};
use super::result::JoinResult;
use super::scanner::TupleScanner;
use super::tuple::{ColumnValue, Tuple};
use super::JoinType;

/// In-memory hash table: hash-key → [(tuple, matched?)].
pub struct HashTable {
    pub buckets: HashMap<u64, Vec<(Tuple, bool)>>,
}

/// Execution mode for the Hash Join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashJoinMode {
    Auto,
    InMemory,
    Grace,
    Hybrid,
}

/// Hash Join executor.
pub struct HashJoinExecutor {
    pub build_table: String,
    pub probe_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
    pub mode: HashJoinMode,
    pub memory_pages: usize,
    pub num_partitions: usize,
}

impl HashJoinExecutor {
    /// Choose in-memory, Grace, or Hybrid based on mode.
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let path = format!("database/base/{}/{}.dat", db, self.build_table);
        let mut f = OpenOptions::new().read(true).write(true).open(&path)?;
        
        let mode = if self.mode == HashJoinMode::Auto {
            if (page_count(&mut f)? as usize) <= self.memory_pages {
                HashJoinMode::InMemory
            } else {
                HashJoinMode::Grace
            }
        } else {
            self.mode
        };

        match mode {
            HashJoinMode::InMemory => self.execute_simple(db, catalog),
            HashJoinMode::Grace => self.execute_grace(db, catalog),
            HashJoinMode::Hybrid => self.execute_hybrid(db, catalog),
            _ => unreachable!(),
        }
    }

    // ── In-memory hash join ──────────────────────────────────────────

    fn execute_simple(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut bscan = TupleScanner::new(db, &self.build_table, catalog)?;
        let mut pscan = TupleScanner::new(db, &self.probe_table, catalog)?;
        let bs = bscan.schema.clone();
        let ps = pscan.schema.clone();

        let (bcol, pcol) = self.resolve_columns();
        let mut ht = self.build_phase(&mut bscan, &bcol);
        let result = self.probe_phase(&mut ht, &mut pscan, &pcol, &bs, &ps);
        Ok(result)
    }

    fn build_phase(&self, scanner: &mut TupleScanner, hash_col: &str) -> HashTable {
        let mut buckets: HashMap<u64, Vec<(Tuple, bool)>> = HashMap::new();
        while let Some(t) = scanner.next_tuple() {
            let key = self.hash_value(t.get_field(hash_col));
            buckets.entry(key).or_default().push((t, false));
        }
        HashTable { buckets }
    }

    fn probe_phase(
        &self, ht: &mut HashTable, scanner: &mut TupleScanner,
        hash_col: &str, build_schema: &[Column], probe_schema: &[Column],
    ) -> JoinResult {
        let build_is_left = self.conditions.first().map(|c| c.left_table == self.build_table).unwrap_or(true);
        let (lt, rt, ls, rs) = if build_is_left {
            (&self.build_table, &self.probe_table, build_schema, probe_schema)
        } else {
            (&self.probe_table, &self.build_table, probe_schema, build_schema)
        };
        let mut result = JoinResult::new(ls, rs, lt, rt);

        while let Some(p) = scanner.next_tuple() {
            let key = self.hash_value(p.get_field(hash_col));
            let mut matched = false;
            if let Some(list) = ht.buckets.get_mut(&key) {
                for (b, bm) in list.iter_mut() {
                    let (l, r) = if build_is_left { (&*b, &p) } else { (&p, &*b) };
                    if evaluate_conditions(&self.conditions, l, r) {
                        result.add(Tuple::merge(l, r));
                        matched = true;
                        *bm = true;
                    }
                }
            }
            if !matched {
                self.emit_unmatched_probe(&p, build_schema, build_is_left, &mut result);
            }
        }

        self.emit_unmatched_build(ht, probe_schema, build_is_left, &mut result);
        result
    }

    // ── Grace hash join ──────────────────────────────────────────────

    fn execute_grace(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        fs::create_dir_all("database/tmp")?;
        let database = catalog.databases.get(db)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Database not found"))?;
        let bs = database.tables.get(&self.build_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Build table not found"))?.columns.clone();
        let ps = database.tables.get(&self.probe_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Probe table not found"))?.columns.clone();

        let (bcol, pcol) = self.resolve_columns();
        let build_is_left = self.conditions.first().map(|c| c.left_table == self.build_table).unwrap_or(true);
        let (lt, rt, ls, rs) = if build_is_left {
            (&self.build_table, &self.probe_table, bs.clone(), ps.clone())
        } else {
            (&self.probe_table, &self.build_table, ps.clone(), bs.clone())
        };

        let np = self.num_partitions.max(2);
        self.partition(db, catalog, &self.build_table, &bcol, &bs, "build", np)?;
        self.partition(db, catalog, &self.probe_table, &pcol, &ps, "probe", np)?;

        let mut result = JoinResult::new(&ls, &rs, lt, rt);

        for i in 0..np {
            let bp = format!("database/tmp/hash_part_build_{}.tmp", i);
            let pp = format!("database/tmp/hash_part_probe_{}.tmp", i);
            if !std::path::Path::new(&bp).exists() || !std::path::Path::new(&pp).exists() { continue; }

            let mut bscan = TupleScanner::from_file(OpenOptions::new().read(true).write(true).open(&bp)?, bs.clone())?;
            let mut pscan = TupleScanner::from_file(OpenOptions::new().read(true).write(true).open(&pp)?, ps.clone())?;

            let mut ht = self.build_phase(&mut bscan, &bcol);

            while let Some(p) = pscan.next_tuple() {
                let key = self.hash_value(p.get_field(&pcol));
                let mut matched = false;
                if let Some(list) = ht.buckets.get_mut(&key) {
                    for (b, bm) in list.iter_mut() {
                        let (l, r) = if build_is_left { (&*b, &p) } else { (&p, &*b) };
                        if evaluate_conditions(&self.conditions, l, r) {
                            result.add(Tuple::merge(l, r));
                            matched = true;
                            *bm = true;
                        }
                    }
                }
                if !matched { self.emit_unmatched_probe(&p, &bs, build_is_left, &mut result); }
            }
            self.emit_unmatched_build(&ht, &ps, build_is_left, &mut result);
        }

        self.cleanup()?;
        Ok(result)
    }

    // ── Hybrid hash join ─────────────────────────────────────────────

    fn execute_hybrid(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        fs::create_dir_all("database/tmp")?;
        let database = catalog.databases.get(db)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Database not found"))?;
        let bs = database.tables.get(&self.build_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Build table not found"))?.columns.clone();
        let ps = database.tables.get(&self.probe_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Probe table not found"))?.columns.clone();

        let (bcol, pcol) = self.resolve_columns();
        let build_is_left = self.conditions.first().map(|c| c.left_table == self.build_table).unwrap_or(true);
        let (lt, rt, ls, rs) = if build_is_left {
            (&self.build_table, &self.probe_table, bs.clone(), ps.clone())
        } else {
            (&self.probe_table, &self.build_table, ps.clone(), bs.clone())
        };

        let np = self.num_partitions.max(2);
        let mut result = JoinResult::new(&ls, &rs, lt, rt);

        // Partition Build Table: Keep part 0 in memory
        let mut ht0: HashMap<u64, Vec<(Tuple, bool)>> = HashMap::new();
        let mut bfiles: Vec<File> = Vec::with_capacity(np - 1);
        for i in 1..np {
            let p = format!("database/tmp/hash_part_build_{}.tmp", i);
            let mut f = OpenOptions::new().create(true).write(true).read(true).truncate(true).open(&p)?;
            let mut h = vec![0u8; PAGE_SIZE];
            h[0..4].copy_from_slice(&1u32.to_le_bytes());
            f.write_all(&h)?; f.flush()?; create_page(&mut f)?;
            bfiles.push(f);
        }

        let mut bscan = TupleScanner::new(db, &self.build_table, catalog)?;
        while let Some(t) = bscan.next_tuple() {
            let key = self.hash_value(t.get_field(&bcol));
            let idx = (key as usize) % np;
            if idx == 0 {
                ht0.entry(key).or_default().push((t, false));
            } else {
                insert_tuple(&mut bfiles[idx - 1], &self.ser(&t, &bs))?;
            }
        }

        // Partition Probe Table: Probe ht0 immediately, otherwise write to disk
        let mut pfiles: Vec<File> = Vec::with_capacity(np - 1);
        for i in 1..np {
            let p = format!("database/tmp/hash_part_probe_{}.tmp", i);
            let mut f = OpenOptions::new().create(true).write(true).read(true).truncate(true).open(&p)?;
            let mut h = vec![0u8; PAGE_SIZE];
            h[0..4].copy_from_slice(&1u32.to_le_bytes());
            f.write_all(&h)?; f.flush()?; create_page(&mut f)?;
            pfiles.push(f);
        }

        let mut pscan = TupleScanner::new(db, &self.probe_table, catalog)?;
        while let Some(p) = pscan.next_tuple() {
            let key = self.hash_value(p.get_field(&pcol));
            let idx = (key as usize) % np;
            if idx == 0 {
                // Probe memory table
                let mut matched = false;
                if let Some(list) = ht0.get_mut(&key) {
                    for (b, bm) in list.iter_mut() {
                        let (l, r) = if build_is_left { (&*b, &p) } else { (&p, &*b) };
                        if evaluate_conditions(&self.conditions, l, r) {
                            result.add(Tuple::merge(l, r));
                            matched = true;
                            *bm = true;
                        }
                    }
                }
                if !matched {
                    self.emit_unmatched_probe(&p, &bs, build_is_left, &mut result);
                }
            } else {
                // Write to disk
                insert_tuple(&mut pfiles[idx - 1], &self.ser(&p, &ps))?;
            }
        }

        // Emit unmatched from ht0
        self.emit_unmatched_build(&mut HashTable { buckets: ht0 }, &ps, build_is_left, &mut result);

        // Process disk partitions 1..np
        for i in 1..np {
            let bp = format!("database/tmp/hash_part_build_{}.tmp", i);
            let pp = format!("database/tmp/hash_part_probe_{}.tmp", i);
            if !std::path::Path::new(&bp).exists() || !std::path::Path::new(&pp).exists() { continue; }

            let mut bscan_p = TupleScanner::from_file(OpenOptions::new().read(true).write(true).open(&bp)?, bs.clone())?;
            let mut pscan_p = TupleScanner::from_file(OpenOptions::new().read(true).write(true).open(&pp)?, ps.clone())?;

            let mut ht = self.build_phase(&mut bscan_p, &bcol);
            while let Some(p) = pscan_p.next_tuple() {
                let key = self.hash_value(p.get_field(&pcol));
                let mut matched = false;
                if let Some(list) = ht.buckets.get_mut(&key) {
                    for (b, bm) in list.iter_mut() {
                        let (l, r) = if build_is_left { (&*b, &p) } else { (&p, &*b) };
                        if evaluate_conditions(&self.conditions, l, r) {
                            result.add(Tuple::merge(l, r));
                            matched = true;
                            *bm = true;
                        }
                    }
                }
                if !matched {
                    self.emit_unmatched_probe(&p, &bs, build_is_left, &mut result);
                }
            }
            self.emit_unmatched_build(&mut ht, &ps, build_is_left, &mut result);
        }

        self.cleanup()?;
        Ok(result)
    }

    // ── Outer-join helpers ───────────────────────────────────────────

    fn emit_unmatched_probe(&self, p: &Tuple, build_schema: &[Column], build_is_left: bool, result: &mut JoinResult) {
        let probe_is_left = !build_is_left;
        let null = Tuple::null_tuple(build_schema);
        match self.join_type {
            JoinType::LeftOuter if probe_is_left   => result.add(Tuple::merge(p, &null)),
            JoinType::RightOuter if !probe_is_left => result.add(Tuple::merge(&null, p)),
            JoinType::FullOuter => {
                let (l, r) = if probe_is_left { (p, &null) } else { (&null, p) };
                result.add(Tuple::merge(l, r));
            }
            _ => {}
        }
    }

    fn emit_unmatched_build(&self, ht: &HashTable, probe_schema: &[Column], build_is_left: bool, result: &mut JoinResult) {
        let should = match self.join_type {
            JoinType::LeftOuter  =>  build_is_left,
            JoinType::RightOuter => !build_is_left,
            JoinType::FullOuter  => true,
            _ => false,
        };
        if !should { return; }
        for list in ht.buckets.values() {
            for (b, bm) in list {
                if !bm {
                    let null = Tuple::null_tuple(probe_schema);
                    let (l, r) = if build_is_left { (b, &null) } else { (&null, b) };
                    result.add(Tuple::merge(l, r));
                }
            }
        }
    }

    // ── Partitioning ─────────────────────────────────────────────────

    fn partition(&self, db: &str, catalog: &Catalog, table: &str, col: &str, schema: &[Column], label: &str, np: usize) -> io::Result<()> {
        let mut scanner = TupleScanner::new(db, table, catalog)?;
        let mut files: Vec<File> = Vec::with_capacity(np);
        for i in 0..np {
            let p = format!("database/tmp/hash_part_{}_{}.tmp", label, i);
            let mut f = OpenOptions::new().create(true).write(true).read(true).truncate(true).open(&p)?;
            let mut h = vec![0u8; PAGE_SIZE];
            h[0..4].copy_from_slice(&1u32.to_le_bytes());
            f.write_all(&h)?; f.flush()?; create_page(&mut f)?;
            files.push(f);
        }
        while let Some(t) = scanner.next_tuple() {
            let key = self.hash_value(t.get_field(col));
            let idx = (key as usize) % np;
            insert_tuple(&mut files[idx], &self.ser(&t, schema))?;
        }
        Ok(())
    }

    // ── Utilities ────────────────────────────────────────────────────

    fn resolve_columns(&self) -> (String, String) {
        let c = self.conditions.first();
        let bcol = c.map(|c| if c.left_table == self.build_table { c.left_col.clone() } else { c.right_col.clone() }).unwrap_or_default();
        let pcol = c.map(|c| if c.left_table == self.probe_table { c.left_col.clone() } else { c.right_col.clone() }).unwrap_or_default();
        (bcol, pcol)
    }

    fn hash_value(&self, val: Option<&ColumnValue>) -> u64 {
        match val {
            Some(ColumnValue::Int(v)) => *v as u64,
            Some(ColumnValue::Text(s)) => {
                let mut h: u64 = 5381;
                for b in s.trim().bytes() { h = h.wrapping_mul(33).wrapping_add(b as u64); }
                h
            }
            _ => 0,
        }
    }

    fn ser(&self, t: &Tuple, schema: &[Column]) -> Vec<u8> {
        let mut b = Vec::new();
        for (i, col) in schema.iter().enumerate() {
            if let Some(val) = t.values.get(i) {
                match col.data_type.as_str() {
                    "INT" => match val { ColumnValue::Int(v) => b.extend_from_slice(&v.to_le_bytes()), _ => b.extend_from_slice(&0i32.to_le_bytes()) },
                    "TEXT" => match val {
                        ColumnValue::Text(s) => { let mut tb = s.as_bytes().to_vec(); tb.resize(10, b' '); tb.truncate(10); b.extend_from_slice(&tb); }
                        _ => b.extend_from_slice(&[b' '; 10]),
                    },
                    _ => {}
                }
            }
        }
        b
    }

    fn cleanup(&self) -> io::Result<()> {
        if let Ok(entries) = fs::read_dir("database/tmp") {
            for e in entries.flatten() { if e.path().is_file() { let _ = fs::remove_file(e.path()); } }
        }
        Ok(())
    }
}
