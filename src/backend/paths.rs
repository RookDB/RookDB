//! Where a data directory keeps its files.
//!
//! Every file the engine reads or writes is named here, relative to the
//! root of a data directory, so the layout is defined in one place. Code
//! asks its [`Engine`](crate::backend::engine::Engine) for a path; nothing
//! else joins directory names.

use std::path::{Path, PathBuf};

/// A table's heap file as recorded in the system tables: relative to the
/// data directory root, so a data directory can be moved.
pub fn relative_heap(db: &str, table: &str) -> String {
    format!("database/base/{}/{}.dat", db, table)
}

/// `path` with `suffix` appended to its file name.
pub fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

/// The file layout under one data directory root.
#[derive(Debug, Clone)]
pub struct Paths {
    root: PathBuf,
}

impl Paths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The data directory root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Directory holding everything the engine stores.
    pub fn data_dir(&self) -> PathBuf {
        self.root.join("database")
    }

    /// Directory holding one sub-directory per database.
    pub fn base_dir(&self) -> PathBuf {
        self.data_dir().join("base")
    }

    /// Directory holding the system tables.
    pub fn system_dir(&self) -> PathBuf {
        self.data_dir().join("system")
    }

    /// The heap file of the system table `name`.
    pub fn system_table(&self, name: &str) -> PathBuf {
        self.system_dir().join(format!("{}.dat", name))
    }

    /// Directory for files that live no longer than one query, such as the
    /// sorted runs of an `ORDER BY` too large for memory.
    pub fn temp_dir(&self) -> PathBuf {
        self.data_dir().join("tmp")
    }

    /// Directory holding one table's operation log.
    pub fn log_dir(&self, db: &str, table: &str) -> PathBuf {
        self.data_dir().join("logs").join(db).join(table)
    }

    /// Directory holding one database's files.
    pub fn database_dir(&self, db: &str) -> PathBuf {
        self.base_dir().join(db)
    }

    /// A table's heap file.
    pub fn heap(&self, db: &str, table: &str) -> PathBuf {
        self.database_dir(db).join(format!("{}.dat", table))
    }

    /// A table's stored column statistics.
    pub fn statistics(&self, db: &str, table: &str) -> PathBuf {
        self.database_dir(db).join(format!("{}.stats", table))
    }

    /// The free-space map fork of a table's heap file.
    pub fn heap_fsm(&self, db: &str, table: &str) -> PathBuf {
        self.database_dir(db).join(format!("{}.dat.fsm", table))
    }

    /// A table's visibility map.
    pub fn visibility_map(&self, db: &str, table: &str) -> PathBuf {
        self.database_dir(db).join(format!("{}_vm", table))
    }

    /// A named index on a table.
    pub fn index(&self, db: &str, table: &str, index: &str) -> PathBuf {
        self.database_dir(db)
            .join(format!("{}.{}.idx", table, index))
    }
}
