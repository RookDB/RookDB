//! Shared helpers for RookDB integration tests.
//!
//! [`TestWorkspace`] gives each test its own data directory under the crate
//! root and opens that directory's engine. The working directory never
//! changes: the test reaches its workspace through [`load_catalog`],
//! [`init_catalog`], [`engine`] and [`path`], which follow the workspace of
//! the current thread, so tests in one binary run in parallel. Dropping the
//! workspace removes it, even when the test panics.
//!
//! Workspace names embed the process ID so different test binaries (which
//! run in parallel as separate processes) never collide.

#![allow(dead_code)]

use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::Arc;

use storage_manager::backend::engine::Engine;

static SWEEP_STALE: std::sync::Once = std::sync::Once::new();

/// Remove workspace directories left behind by *dead* processes.
///
/// Guards cannot clean up when a test run is killed outright (SIGKILL /
/// timeout / IDE stop button), so this runs once per process before the
/// first workspace is created: any `database_<prefix>_p<PID>_<tag>` whose
/// PID no longer exists is stale by definition and gets removed. Live
/// processes' workspaces are never touched, keeping parallel binaries safe.
fn sweep_stale_workspaces() {
    SWEEP_STALE.call_once(|| {
        let Ok(entries) = std::fs::read_dir(".") else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };

            // Strict shape check — never touch anything we can't positively
            // attribute to a test workspace (e.g. real `database/` data).
            let parts: Vec<&str> = name.split('_').collect();
            if parts.len() < 4 || !name.starts_with("database_") {
                continue;
            }
            let Some(pid_str) = parts[2].strip_prefix('p') else {
                continue;
            };
            let Ok(pid) = pid_str.parse::<u32>() else {
                continue;
            };

            let alive = std::path::Path::new(&format!("/proc/{pid}")).exists();
            if !alive {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    });
}

pub struct TestWorkspace {
    /// Absolute path of the workspace, removed on drop.
    path: PathBuf,
    /// The workspace's engine, kept open for the test.
    engine: Arc<Engine>,
}

thread_local! {
    /// The engine of the workspace the current test opened. Each test runs
    /// on its own thread, so tests in one binary can run in parallel.
    static CURRENT: RefCell<Option<Arc<Engine>>> = const { RefCell::new(None) };
}

impl TestWorkspace {
    /// Create `<crate root>/database_<prefix>_p<pid>_<tag>/`, open its engine
    /// and bootstrap the system catalog inside it. The working directory is
    /// left alone: the test reaches the workspace through [`load_catalog`],
    /// [`init_catalog`], [`engine`] and [`path`].
    ///
    /// The returned guard must be bound (`let _ws = ...`) so it lives until
    /// the end of the test; dropping it removes the workspace.
    pub fn new(prefix: &str, tag: &str) -> Self {
        sweep_stale_workspaces();
        let path = std::env::current_dir().expect("read cwd").join(format!(
            "database_{}_p{}_{}",
            prefix,
            std::process::id(),
            tag
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create workspace");

        let engine = Engine::open(&path);
        storage_manager::catalog::init_catalog_in(&engine).expect("init catalog");
        CURRENT.with(|c| *c.borrow_mut() = Some(Arc::clone(&engine)));
        Self { path, engine }
    }

    /// Absolute path of the workspace.
    #[allow(dead_code)]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// The workspace's engine.
    #[allow(dead_code)]
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = None);
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// The engine of the current test's workspace.
#[allow(dead_code)]
pub fn engine() -> Arc<Engine> {
    CURRENT
        .with(|c| c.borrow().clone())
        .expect("no TestWorkspace is open on this thread")
}

/// The catalog of the current test's workspace.
#[allow(dead_code)]
pub fn load_catalog() -> storage_manager::catalog::Catalog {
    storage_manager::catalog::load_catalog_in(&engine())
}

/// Bootstrap the current workspace's system catalog (idempotent).
#[allow(dead_code)]
pub fn init_catalog() {
    storage_manager::catalog::init_catalog_in(&engine()).expect("init catalog")
}

/// `relative` inside the current test's workspace.
#[allow(dead_code)]
pub fn path(relative: impl AsRef<std::path::Path>) -> PathBuf {
    engine().root().join(relative)
}

/// The page count stored in a heap file's header (bytes 0..4).
pub fn page_count(file: &mut std::fs::File) -> std::io::Result<u32> {
    header_u32(file, 0)
}

/// The dead-tuple count stored in a heap file's header (bytes 20..24).
pub fn read_dead_tuple_count(file: &mut std::fs::File) -> std::io::Result<u32> {
    header_u32(file, 20)
}

fn header_u32(file: &mut std::fs::File, offset: u64) -> std::io::Result<u32> {
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}
