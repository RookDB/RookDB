//! Sort-Merge Join executor.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, OpenOptions};
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

pub struct SortRun { pub file_path: String, pub page_count: u32 }

pub struct SMJExecutor {
    pub left_table: String,
    pub right_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
    pub memory_pages: usize,
}

struct MergeItem { tuple: Tuple, run_idx: usize, sort_col_idx: usize }
impl PartialEq for MergeItem {
    fn eq(&self, other: &Self) -> bool { self.tuple.values[self.sort_col_idx].eq_value(&other.tuple.values[self.sort_col_idx]) }
}
impl Eq for MergeItem {}
impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.tuple.values[self.sort_col_idx].partial_cmp_values(&self.tuple.values[self.sort_col_idx])
    }
}
impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> Ordering { self.partial_cmp(other).unwrap_or(Ordering::Equal) }
}

impl SMJExecutor {
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        fs::create_dir_all("database/tmp")?;
        let database = catalog.databases.get(db)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Database not found"))?;
        let left_schema = database.tables.get(&self.left_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Left table not found"))?.columns.clone();
        let right_schema = database.tables.get(&self.right_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Right table not found"))?.columns.clone();
        let left_col = self.conditions.first().map(|c| c.left_col.clone()).unwrap_or_default();
        let right_col = self.conditions.first().map(|c| c.right_col.clone()).unwrap_or_default();
        let lr = self.sort_relation(&self.left_table, db, catalog, &left_col)?;
        let rr = self.sort_relation(&self.right_table, db, catalog, &right_col)?;
        let lm = self.merge_all(lr, &left_schema, &left_col)?;
        let rm = self.merge_all(rr, &right_schema, &right_col)?;
        let result = self.merge_join(&lm, &rm, &left_schema, &right_schema, &left_col, &right_col)?;
        let _ = self.cleanup();
        Ok(result)
    }

    fn sort_relation(&self, table: &str, db: &str, catalog: &Catalog, sort_col: &str) -> io::Result<Vec<SortRun>> {
        let mut scanner = TupleScanner::new(db, table, catalog)?;
        let schema = scanner.schema.clone();
        let mut runs = Vec::new();
        let mut rid = 0u32;
        let limit = self.memory_pages * 100;
        loop {
            let mut batch = Vec::with_capacity(limit);
            for _ in 0..limit { match scanner.next_tuple() { Some(t) => batch.push(t), None => break } }
            if batch.is_empty() { break; }
            batch.sort_by(|a, b| match (a.get_field(sort_col), b.get_field(sort_col)) {
                (Some(va), Some(vb)) => va.partial_cmp_values(vb).unwrap_or(Ordering::Equal), _ => Ordering::Equal,
            });
            let path = format!("database/tmp/sort_run_{}_{}.tmp", table, rid);
            let mut f = self.create_temp(&path)?;
            for t in &batch { insert_tuple(&mut f, &self.ser(t, &schema))?; }
            let pc = page_count(&mut f)?;
            runs.push(SortRun { file_path: path, page_count: pc });
            rid += 1;
        }
        Ok(runs)
    }

    fn merge_all(&self, mut runs: Vec<SortRun>, schema: &[Column], sort_col: &str) -> io::Result<SortRun> {
        if runs.is_empty() {
            let p = format!("database/tmp/empty_{}.tmp", ts());
            let mut f = self.create_temp(&p)?;
            let pc = page_count(&mut f)?;
            return Ok(SortRun { file_path: p, page_count: pc });
        }
        let sci = schema.iter().position(|c| c.name == sort_col).unwrap_or(0);
        while runs.len() > 1 {
            let mut next = Vec::new();
            for chunk in runs.chunks(16) {
                if chunk.len() == 1 { next.push(SortRun { file_path: chunk[0].file_path.clone(), page_count: chunk[0].page_count }); continue; }
                let mut scanners = Vec::new();
                for r in chunk {
                    let f = OpenOptions::new().read(true).write(true).open(&r.file_path)?;
                    scanners.push(TupleScanner::from_file(f, schema.to_vec())?);
                }
                let mut heap = BinaryHeap::new();
                for (i, s) in scanners.iter_mut().enumerate() {
                    if let Some(t) = s.next_tuple() { heap.push(MergeItem { tuple: t, run_idx: i, sort_col_idx: sci }); }
                }
                let mp = format!("database/tmp/merged_{}.tmp", ts());
                let mut mf = self.create_temp(&mp)?;
                while let Some(item) = heap.pop() {
                    insert_tuple(&mut mf, &self.ser(&item.tuple, schema))?;
                    if let Some(t) = scanners[item.run_idx].next_tuple() {
                        heap.push(MergeItem { tuple: t, run_idx: item.run_idx, sort_col_idx: sci });
                    }
                }
                let pc = page_count(&mut mf)?;
                next.push(SortRun { file_path: mp, page_count: pc });
                for r in chunk { let _ = fs::remove_file(&r.file_path); }
            }
            runs = next;
        }
        Ok(runs.into_iter().next().unwrap())
    }

    fn merge_join(&self, lr: &SortRun, rr: &SortRun, ls: &[Column], rs: &[Column], lc: &str, rc: &str) -> io::Result<JoinResult> {
        let mut lscan = TupleScanner::from_file(OpenOptions::new().read(true).write(true).open(&lr.file_path)?, ls.to_vec())?;
        let mut rscan = TupleScanner::from_file(OpenOptions::new().read(true).write(true).open(&rr.file_path)?, rs.to_vec())?;
        let mut result = JoinResult::new(ls, rs, &self.left_table, &self.right_table);
        let mut cl = lscan.next_tuple();
        let mut cr = rscan.next_tuple();
        let mut rg: Vec<(Tuple, bool)> = Vec::new();
        while let Some(l) = &cl {
            let lv = match l.get_field(lc) {
                Some(v) => v,
                None => {
                    if self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter {
                        result.add(Tuple::merge(l, &Tuple::null_tuple(rs)));
                    }
                    cl = lscan.next_tuple(); continue;
                }
            };
            if !rg.is_empty() && rg[0].0.get_field(rc).unwrap().eq_value(lv) {
                let mut ml = false;
                for (r, rm) in &mut rg { if evaluate_conditions(&self.conditions, l, r) { result.add(Tuple::merge(l, r)); ml = true; *rm = true; } }
                if !ml && (self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter) { result.add(Tuple::merge(l, &Tuple::null_tuple(rs))); }
                cl = lscan.next_tuple(); continue;
            } else if !rg.is_empty() {
                if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter {
                    for (r, rm) in &rg { if !rm { result.add(Tuple::merge(&Tuple::null_tuple(ls), r)); } }
                }
                rg.clear();
            }
            let mut adv = false;
            while let Some(r) = &cr {
                let rv = match r.get_field(rc) {
                    Some(v) => v,
                    None => {
                        if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter { result.add(Tuple::merge(&Tuple::null_tuple(ls), r)); }
                        cr = rscan.next_tuple(); continue;
                    }
                };
                match lv.partial_cmp_values(rv) {
                    Some(Ordering::Less) => break,
                    Some(Ordering::Greater) => {
                        if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter { result.add(Tuple::merge(&Tuple::null_tuple(ls), r)); }
                        cr = rscan.next_tuple();
                    }
                    Some(Ordering::Equal) => {
                        rg.clear(); let key = rv.clone(); rg.push((r.clone(), false));
                        cr = rscan.next_tuple();
                        while let Some(nr) = &cr {
                            if let Some(nv) = nr.get_field(rc) { if nv.eq_value(&key) { rg.push((nr.clone(), false)); cr = rscan.next_tuple(); } else { break; } } else { break; }
                        }
                        adv = true; break;
                    }
                    None => {
                        if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter { result.add(Tuple::merge(&Tuple::null_tuple(ls), r)); }
                        cr = rscan.next_tuple();
                    }
                }
            }
            if adv {
                let mut ml = false;
                for (r, rm) in &mut rg { if evaluate_conditions(&self.conditions, l, r) { result.add(Tuple::merge(l, r)); ml = true; *rm = true; } }
                if !ml && (self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter) { result.add(Tuple::merge(l, &Tuple::null_tuple(rs))); }
            } else if self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter { result.add(Tuple::merge(l, &Tuple::null_tuple(rs))); }
            cl = lscan.next_tuple();
        }
        if !rg.is_empty() && (self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter) {
            for (r, rm) in &rg { if !rm { result.add(Tuple::merge(&Tuple::null_tuple(ls), r)); } }
        }
        if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter {
            while let Some(r) = cr { result.add(Tuple::merge(&Tuple::null_tuple(ls), &r)); cr = rscan.next_tuple(); }
        }
        Ok(result)
    }

    fn create_temp(&self, path: &str) -> io::Result<std::fs::File> {
        let mut f = OpenOptions::new().create(true).write(true).read(true).truncate(true).open(path)?;
        let mut h = vec![0u8; PAGE_SIZE]; h[0..4].copy_from_slice(&1u32.to_le_bytes());
        f.write_all(&h)?; f.flush()?; create_page(&mut f)?; Ok(f)
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

fn ts() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    format!("{:x}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos())
}
