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

    /// The file a process holds locked while it has the directory open.
    pub fn lock_file(&self) -> PathBuf {
        self.data_dir().join("rookdb.lock")
    }

    /// Directory holding the system tables.
    pub fn system_dir(&self) -> PathBuf {
        self.data_dir().join("system")
    }

    /// Where a catalog save builds the next system directory before swapping
    /// it in.
    pub fn system_staging_dir(&self) -> PathBuf {
        self.data_dir().join("system.new")
    }

    /// Where a catalog save parks the previous system directory during the
    /// swap.
    pub fn system_old_dir(&self) -> PathBuf {
        self.data_dir().join("system.old")
    }

    /// The copy of the system directory a statement that changes the catalog
    /// keeps until it finishes; see `system_table::begin_catalog_change`.
    pub fn system_undo_dir(&self) -> PathBuf {
        self.data_dir().join("system.undo")
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

    /// The file `<db dir>/<table><suffix>`, built in one allocation: a
    /// table's paths are asked for on every row an INSERT writes.
    fn table_file(&self, db: &str, table: &str, suffix: &str) -> PathBuf {
        let len = self.root.as_os_str().len() + db.len() + table.len() + suffix.len();
        let mut path = PathBuf::with_capacity(len + "/database/base//".len());
        path.push(&self.root);
        path.push("database");
        path.push("base");
        path.push(db);
        path.push(table);
        path.as_mut_os_string().push(suffix);
        path
    }

    /// A table's heap file.
    pub fn heap(&self, db: &str, table: &str) -> PathBuf {
        self.table_file(db, table, ".dat")
    }

    /// A table's stored column statistics.
    pub fn statistics(&self, db: &str, table: &str) -> PathBuf {
        self.table_file(db, table, ".stats")
    }

    /// The free-space map fork of a table's heap file.
    pub fn heap_fsm(&self, db: &str, table: &str) -> PathBuf {
        self.table_file(db, table, ".dat.fsm")
    }

    /// A table's visibility map.
    pub fn visibility_map(&self, db: &str, table: &str) -> PathBuf {
        self.table_file(db, table, "_vm")
    }

    /// A named index on a table.
    pub fn index(&self, db: &str, table: &str, index: &str) -> PathBuf {
        let mut path = self.table_file(db, table, ".");
        path.as_mut_os_string().push(index);
        path.as_mut_os_string().push(".idx");
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_files_match_the_joined_layout() {
        for root in ["", "data", "/srv/rook"] {
            let p = Paths::new(root);
            let dir = Path::new(root).join("database").join("base").join("db");
            assert_eq!(p.heap("db", "t"), dir.join("t.dat"));
            assert_eq!(p.statistics("db", "t"), dir.join("t.stats"));
            assert_eq!(p.heap_fsm("db", "t"), dir.join("t.dat.fsm"));
            assert_eq!(p.visibility_map("db", "t"), dir.join("t_vm"));
            assert_eq!(p.index("db", "t", "ix"), dir.join("t.ix.idx"));
            assert_eq!(p.database_dir("db"), dir);
        }
    }
}
