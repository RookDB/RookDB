//! The spill layer: framing, scoped directories, and cleanup.
//!
//! Three defects in the previous implementation are addressed here, and each
//! has a test that would fail if it came back: run files that could be read
//! under the wrong schema, fixed file names that made a self-join's two sides
//! collide, and a `cleanup()` that deleted every file in the shared temp
//! directory rather than its own.

#[path = "join_common/mod.rs"]
mod common;

use std::time::Duration;

use common::TempDb;
use storage_manager::join::spill::sweep_orphans;
use storage_manager::join::{
    JoinError, MemoryAccountant, RowBuffer, RowBufferBuilder, RunWriter, SpillScope,
};

const FINGERPRINT: u64 = 0xABCD_1234_5678_9ABC;

fn rows(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("row-{i}-{}", "x".repeat(i % 17)).into_bytes())
        .collect()
}

// ── Framing ──────────────────────────────────────────────────────────────────

/// Rows come back byte-identical, including empty ones. Length framing is not
/// an optimisation: a row read out of a padded buffer decodes incorrectly,
/// because its last variable-length payload is defined by the slice length.
#[test]
fn rows_round_trip_byte_for_byte() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");

    let mut written = rows(200);
    written.push(Vec::new());
    written.push(vec![0u8; 4096]);

    let mut writer = RunWriter::create(&scope, "test", FINGERPRINT).expect("writer");
    for row in &written {
        writer.write_row(row).expect("write");
    }
    assert_eq!(writer.rows(), written.len() as u64);
    let handle = writer.finish().expect("finish");

    let read: Vec<Vec<u8>> = handle
        .reader()
        .expect("reader")
        .map(|row| row.expect("read"))
        .collect();

    assert_eq!(read, written);
    assert_eq!(handle.rows(), written.len() as u64);
}

/// A run can be read as many times as needed - which is what lets a sort-merge
/// join replay a duplicate group once per row of the other side.
#[test]
fn a_run_can_be_read_repeatedly() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");
    let written = rows(20);

    let mut writer = RunWriter::create(&scope, "test", FINGERPRINT).expect("writer");
    for row in &written {
        writer.write_row(row).expect("write");
    }
    let handle = writer.finish().expect("finish");

    for _ in 0..3 {
        let read: Vec<Vec<u8>> = handle
            .reader()
            .expect("reader")
            .map(|row| row.expect("read"))
            .collect();
        assert_eq!(read, written);
    }
}

/// Write one run, then edit its header on disk to stand in for a run produced
/// under a different schema or by something else entirely.
fn run_with_patched_header(
    scope: &std::sync::Arc<SpillScope>,
    patch: impl Fn(&mut [u8]),
) -> JoinError {
    let mut writer = RunWriter::create(scope, "test", FINGERPRINT).expect("writer");
    writer.write_row(b"payload").expect("write");
    let handle = writer.finish().expect("finish");

    assert!(
        handle.reader().is_ok(),
        "the run must be readable before the header is damaged"
    );

    let mut bytes = std::fs::read(handle.path()).expect("read");
    patch(&mut bytes);
    std::fs::write(handle.path(), &bytes).expect("write back");

    handle
        .reader()
        .err()
        .expect("a damaged header must be refused")
}

/// Reading a run under a different schema would decode every row silently
/// wrong. The fingerprint in the header makes that a hard error - the exact
/// failure the old fixed-name run files hid when a self-join's two sides
/// overwrote each other.
#[test]
fn a_run_written_for_another_schema_is_refused() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");

    // Bytes 8..16 hold the schema fingerprint.
    let err = run_with_patched_header(&scope, |bytes| {
        bytes[8..16].copy_from_slice(&(FINGERPRINT ^ 0xFFFF).to_le_bytes());
    });

    let rendered = err.to_string();
    assert!(
        rendered.contains("different schema"),
        "the error should name the cause: {rendered}"
    );
}

/// A file that is not a run at all is rejected on its magic, before anything
/// tries to interpret its contents as rows.
#[test]
fn a_file_that_is_not_a_run_is_refused() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");

    let err = run_with_patched_header(&scope, |bytes| {
        bytes[..8].copy_from_slice(b"NOTARUN!");
    });

    assert!(err.to_string().contains("not a join run file"), "{err}");
}

/// A run cut short mid-row reports where it failed rather than returning a
/// partial row.
#[test]
fn a_truncated_run_reports_an_error() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");

    let mut writer = RunWriter::create(&scope, "test", FINGERPRINT).expect("writer");
    writer.write_row(b"0123456789").expect("write");
    let handle = writer.finish().expect("finish");

    let complete = std::fs::read(handle.path()).expect("read");
    std::fs::write(handle.path(), &complete[..complete.len() - 4]).expect("truncate");

    let outcome: Result<Vec<Vec<u8>>, JoinError> = handle.reader().expect("reader").collect();
    let err = outcome.expect_err("a truncated run must report an error");
    assert!(err.to_string().contains("ends inside a row"), "{err}");
}

// ── Scoped cleanup ───────────────────────────────────────────────────────────

/// Each scope owns exactly one directory and removes exactly that directory.
#[test]
fn dropping_a_scope_removes_only_its_own_directory() {
    let db = TempDb::new();

    let first = SpillScope::create(db.path()).expect("scope");
    let second = SpillScope::create(db.path()).expect("scope");

    let first_dir = first.dir().to_path_buf();
    let second_dir = second.dir().to_path_buf();
    assert_ne!(
        first_dir, second_dir,
        "two operators must never share a directory"
    );

    let mut writer = RunWriter::create(&first, "a", FINGERPRINT).expect("writer");
    writer.write_row(b"data").expect("write");
    let _handle = writer.finish().expect("finish");

    let mut writer = RunWriter::create(&second, "b", FINGERPRINT).expect("writer");
    writer.write_row(b"data").expect("write");
    let sibling = writer.finish().expect("finish");

    drop(first);
    drop(_handle);

    assert!(!first_dir.exists(), "the dropped scope must be removed");
    assert!(
        second_dir.exists(),
        "a sibling operator's directory must survive"
    );
    assert!(
        sibling.reader().is_ok(),
        "the sibling's run must still be readable"
    );
}

/// A directory outlives any handle still referring to it, so a partition
/// cannot be deleted while another is being read.
#[test]
fn a_directory_outlives_its_handles() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");
    let dir = scope.dir().to_path_buf();

    let mut writer = RunWriter::create(&scope, "a", FINGERPRINT).expect("writer");
    writer.write_row(b"data").expect("write");
    let handle = writer.finish().expect("finish");

    drop(scope);
    assert!(dir.exists(), "a live handle keeps the directory alive");
    assert!(handle.reader().is_ok());

    drop(handle);
    assert!(!dir.exists(), "the last handle going away removes it");
}

/// Cleanup runs while the stack unwinds, so a panic mid-join leaves nothing
/// behind.
#[test]
fn a_panic_still_cleans_up() {
    let db = TempDb::new();
    let root = db.path().to_path_buf();

    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(|| {
        let scope = SpillScope::create(&root).expect("scope");
        let mut writer = RunWriter::create(&scope, "a", FINGERPRINT).expect("writer");
        writer.write_row(b"data").expect("write");
        let _handle = writer.finish().expect("finish");
        panic!("operator failed mid-join");
    });
    std::panic::set_hook(previous_hook);

    assert!(outcome.is_err(), "the panic should have propagated");

    let leftovers: Vec<_> = std::fs::read_dir(db.path())
        .expect("list")
        .flatten()
        .filter(|e| e.path().is_dir())
        .collect();
    assert!(
        leftovers.is_empty(),
        "unwinding must remove the scope: {leftovers:?}"
    );
}

// ── Orphan sweeping ──────────────────────────────────────────────────────────

/// Sweeping only ever removes directories it recognises, whose owning process
/// is gone. It never removes a live process's directory, and never touches
/// anything it did not create.
#[test]
fn sweeping_removes_only_recognised_dead_directories() {
    let db = TempDb::new();
    let root = db.path();

    // A directory belonging to this live process.
    let mine = SpillScope::create(root).expect("scope");
    let mine_dir = mine.dir().to_path_buf();

    // A plausible directory from a process that no longer exists. PID 0 is
    // never a running user process.
    let dead = root.join("join-0-1000-7");
    std::fs::create_dir_all(&dead).expect("create");
    std::fs::write(dead.join("x.run"), b"stale").expect("write");

    // Something else entirely, which must be left alone.
    let foreign = root.join("someone-elses-data");
    std::fs::create_dir_all(&foreign).expect("create");
    std::fs::write(foreign.join("important"), b"keep me").expect("write");

    let removed = sweep_orphans(root, Duration::from_secs(1)).expect("sweep");

    assert_eq!(removed, 1, "only the dead directory should go");
    assert!(!dead.exists(), "the stale directory must be removed");
    assert!(mine_dir.exists(), "this process's directory must survive");
    assert!(
        foreign.exists(),
        "unrecognised directories must be left alone"
    );
    assert!(foreign.join("important").exists());
}

#[test]
fn sweeping_a_missing_root_is_not_an_error() {
    let db = TempDb::new();
    let missing = db.path().join("never-created");
    assert_eq!(
        sweep_orphans(&missing, Duration::from_secs(0)).expect("sweep"),
        0
    );
}

// ── Buffers that spill ───────────────────────────────────────────────────────

/// Under budget the rows stay in memory; the reader is the same either way.
#[test]
fn a_small_buffer_stays_in_memory() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");
    let budget = MemoryAccountant::new(1024 * 1024);

    let mut builder = RowBufferBuilder::new(&scope, "group", FINGERPRINT);
    let written = rows(10);
    for row in &written {
        builder.push(row, &budget).expect("push");
    }
    assert!(!builder.spilled());

    let buffer = builder.finish(&budget).expect("finish");
    assert!(matches!(buffer, RowBuffer::Memory(_)));
    assert_eq!(buffer.len(), 10);
    assert_eq!(
        budget.used(),
        0,
        "finishing an in-memory buffer hands its rows over and releases the charge"
    );

    let read: Vec<Vec<u8>> = buffer
        .reader()
        .expect("reader")
        .map(|row| row.expect("read"))
        .collect();
    assert_eq!(read, written);
}

/// Over budget the buffer moves to disk mid-stream, and the rows already held
/// move with it. This is what bounds a single hot key in a sort-merge join.
#[test]
fn a_large_buffer_spills_without_losing_rows() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");
    let budget = MemoryAccountant::new(512);

    let mut builder = RowBufferBuilder::new(&scope, "group", FINGERPRINT);
    let written = rows(500);
    for row in &written {
        builder.push(row, &budget).expect("push");
    }
    assert!(builder.spilled(), "500 rows must not fit in 512 bytes");

    let buffer = builder.finish(&budget).expect("finish");
    assert!(buffer.spilled());
    assert_eq!(buffer.len(), written.len() as u64);
    assert_eq!(
        budget.used(),
        0,
        "spilling must release everything it was holding"
    );

    // Read it twice: order and content are preserved, and it stays readable.
    for _ in 0..2 {
        let read: Vec<Vec<u8>> = buffer
            .reader()
            .expect("reader")
            .map(|row| row.expect("read"))
            .collect();
        assert_eq!(read, written, "spilling must not reorder or drop rows");
    }
}

/// The in-memory and spilled paths must be indistinguishable to a consumer.
#[test]
fn spilled_and_resident_buffers_agree() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");
    let written = rows(120);

    let collect_with = |budget_bytes: u64| {
        let budget = MemoryAccountant::new(budget_bytes);
        let mut builder = RowBufferBuilder::new(&scope, "group", FINGERPRINT);
        for row in &written {
            builder.push(row, &budget).expect("push");
        }
        let spilled = builder.spilled();
        let buffer = builder.finish(&budget).expect("finish");
        let read: Vec<Vec<u8>> = buffer
            .reader()
            .expect("reader")
            .map(|row| row.expect("read"))
            .collect();
        (spilled, read)
    };

    let (resident_spilled, resident) = collect_with(1024 * 1024);
    let (disk_spilled, disk) = collect_with(256);

    assert!(!resident_spilled);
    assert!(disk_spilled);
    assert_eq!(resident, disk);
    assert_eq!(resident, written);
}

#[test]
fn an_empty_buffer_is_valid() {
    let db = TempDb::new();
    let scope = SpillScope::create(db.path()).expect("scope");
    let budget = MemoryAccountant::new(1024);

    let buffer = RowBufferBuilder::new(&scope, "group", FINGERPRINT)
        .finish(&budget)
        .expect("finish");
    assert!(buffer.is_empty());
    assert_eq!(buffer.reader().expect("reader").count(), 0);
}
