//! Duplicate management commands (options 20-23).

use std::io::{self, Write};

use storage_manager::catalog::load_catalog;
use storage_manager::executor::duplicate::{
    build_duplicate_index, copy_deduped, copy_duplicates_only, load_duplicate_index,
};

fn prompt(msg: &str) -> io::Result<String> {
    print!("{}", msg);
    io::stdout().flush()?;
    let mut s = String::new();
    io::stdin().read_line(&mut s)?;
    Ok(s.trim().to_string())
}

fn require_db(current_db: &Option<String>) -> Option<String> {
    match current_db {
        Some(db) => Some(db.clone()),
        None => { println!("  No database selected."); None }
    }
}

/// Option 20: scan table, build .dup sidecar, print report.
pub fn find_duplicates_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }

    println!("  Scanning for duplicates (this reads the full table)...");
    let catalog = load_catalog();
    let report = build_duplicate_index(&catalog, &db, &table)?;
    println!();
    report.print();
    println!(
        "\n  Sidecar index saved to: database/base/{}/{}.dup",
        db, table
    );
    Ok(())
}

/// Option 21: write {table}_dedup.dat without duplicates.
pub fn export_deduped_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }

    // Ensure .dup index exists
    let idx = load_duplicate_index(&db, &table)?;
    if idx.is_empty() {
        println!("  No .dup index found. Running duplicate scan first...");
        let catalog = load_catalog();
        build_duplicate_index(&catalog, &db, &table)?;
    }

    let catalog = load_catalog();
    println!("  Writing deduped file...");
    let n = copy_deduped(&catalog, &db, &table)?;
    println!("  Done. {} unique rows exported.", n);
    Ok(())
}

/// Option 22: write {table}_dups_only.dat containing only duplicates.
pub fn export_dups_only_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }

    let idx = load_duplicate_index(&db, &table)?;
    if idx.is_empty() {
        println!("  No .dup index found. Running duplicate scan first...");
        let catalog = load_catalog();
        build_duplicate_index(&catalog, &db, &table)?;
    }

    let catalog = load_catalog();
    println!("  Writing duplicates-only file...");
    let n = copy_duplicates_only(&catalog, &db, &table)?;
    println!("  Done. {} duplicate rows exported.", n);
    Ok(())
}

/// Option 23: show the raw .dup sidecar contents.
pub fn show_dup_index_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }

    let locs = load_duplicate_index(&db, &table)?;
    if locs.is_empty() {
        println!("  No .dup index for '{}.{}'. Run option 20 first.", db, table);
        return Ok(());
    }
    println!("  Duplicate index for '{}.{}': {} entries", db, table, locs.len());
    for loc in locs.iter().take(50) {
        println!("    page={} slot={}", loc.page_num, loc.slot_num);
    }
    if locs.len() > 50 {
        println!("    ... and {} more", locs.len() - 50);
    }
    Ok(())
}
