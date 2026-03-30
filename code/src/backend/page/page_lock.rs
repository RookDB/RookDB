//! Page-level write locking to prevent race conditions between foreground
//! operations (insert, update, delete) and background compaction.
//!
//! Usage:
//!   - Before reading/modifying a page: acquire_write_lock(file_id, page_id)
//!   - After finish: release_write_lock(file_id, page_id)
//!
//! Multiple threads can hold locks on different pages, but only one can
//! hold a lock on a specific (file_id, page_id) pair.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

type FileIdentity = u64;
type PageId = u32;

/// Composite key for page locks: (file_identity, page_id)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct PageKey {
    file_id: FileIdentity,
    page_id: PageId,
}

/// Global page lock registry.
/// Maps each (file_id, page_id) to a mutex protecting access.
struct PageLockRegistry {
    locks: HashMap<PageKey, Arc<Mutex<()>>>,
}

impl PageLockRegistry {
    fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    /// Get or create a lock for the given page.
    /// Returns the Arc<Mutex<()>> that protects that page.
    fn get_or_create_lock(&mut self, key: PageKey) -> Arc<Mutex<()>> {
        self.locks
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

static REGISTRY: OnceLock<Mutex<PageLockRegistry>> = OnceLock::new();

fn registry() -> &'static Mutex<PageLockRegistry> {
    REGISTRY.get_or_init(|| Mutex::new(PageLockRegistry::new()))
}

/// Holds an exclusive write lock on a page; automatically releases on drop.
pub struct PageWriteLock {
    #[allow(dead_code)]
    key: PageKey,
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl PageWriteLock {
    /// Acquire an exclusive write lock on a page.
    /// Blocks until no other thread holds the lock.
    pub fn acquire(file_id: FileIdentity, page_id: PageId) -> Self {
        let key = PageKey { file_id, page_id };

        let lock_arc = {
            let mut reg = registry().lock().unwrap();
            reg.get_or_create_lock(key)
        };

        let guard = lock_arc.lock().unwrap();

        PageWriteLock {
            key,
            _guard: unsafe { std::mem::transmute(guard) },
        }
    }
}

impl Drop for PageWriteLock {
    fn drop(&mut self) {
        // Lock is automatically released when _guard is dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_page_lock_sequential() {
        let lock1 = PageWriteLock::acquire(1, 100);
        drop(lock1);

        // Re-acquiring the same page lock after release should succeed.
        let lock2 = PageWriteLock::acquire(1, 100);
        drop(lock2);
    }

    #[test]
    fn test_page_lock_different_pages() {
        let lock1 = PageWriteLock::acquire(1, 100);
        let lock2 = PageWriteLock::acquire(1, 101); // Different page, should not block
        drop(lock1);
        drop(lock2);
    }

    #[test]
    fn test_page_lock_mutual_exclusion() {
        let file_id = 42u64;
        let page_id = 10u32;
        let flag = Arc::new(Mutex::new(false));
        let flag_clone = Arc::clone(&flag);

        let handle = thread::spawn(move || {
            let _lock = PageWriteLock::acquire(file_id, page_id);
            *flag_clone.lock().unwrap() = true;
            thread::sleep(std::time::Duration::from_millis(100));
        });

        thread::sleep(std::time::Duration::from_millis(10));
        let _lock = PageWriteLock::acquire(file_id, page_id);
        assert!(*flag.lock().unwrap()); // Flag should be set by now

        handle.join().unwrap();
    }
}
