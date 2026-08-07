//! Memory accounting for join operators.
//!
//! Rust exposes no allocator introspection, so this is a deliberate
//! over-estimate of what an operator is holding, not a measurement. Its job is
//! to trigger a strategy change - spill, repartition, fall back to a nested
//! loop - well before the real allocator is under pressure. Treating it as a
//! hard limit would be wrong; treating it as a budget is exactly right.
//!
//! Accountants form a tree so a hash join's resident partition can be charged
//! against the operator's budget. There is no process-wide accountant: a
//! global would couple every operator, and every test, to every other.

use std::cell::Cell;
use std::fmt;
use std::rc::Rc;

/// Per-row bookkeeping beyond the row bytes themselves: the `Vec` header and
/// allocator rounding.
const ROW_OVERHEAD: u64 = 32;

/// Cost of one hash-table entry beyond its key and row: the bucket slot, the
/// per-key `Vec`, and slack at a 0.75 load factor.
pub const HASH_ENTRY_OVERHEAD: u64 = 48;

/// What holding one serialized row actually costs.
pub fn row_footprint(row_len: usize) -> u64 {
    row_len as u64 + ROW_OVERHEAD
}

/// Returned when a charge would exceed the budget. Not an error in itself -
/// operators respond by changing strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverBudget {
    pub requested: u64,
    pub used: u64,
    pub budget: u64,
}

impl fmt::Display for OverBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "needed {} more bytes with {} of {} already held",
            self.requested, self.used, self.budget
        )
    }
}

#[derive(Debug)]
pub struct MemoryAccountant {
    budget: Cell<u64>,
    used: Cell<u64>,
    peak: Cell<u64>,
    parent: Option<Rc<MemoryAccountant>>,
}

impl MemoryAccountant {
    pub fn new(budget: u64) -> Rc<Self> {
        Rc::new(Self {
            budget: Cell::new(budget),
            used: Cell::new(0),
            peak: Cell::new(0),
            parent: None,
        })
    }

    /// A sub-budget that also draws on its parent, so nested structures cannot
    /// collectively exceed the operator's allowance.
    pub fn child(parent: &Rc<MemoryAccountant>, budget: u64) -> Rc<Self> {
        Rc::new(Self {
            budget: Cell::new(budget),
            used: Cell::new(0),
            peak: Cell::new(0),
            parent: Some(Rc::clone(parent)),
        })
    }

    pub fn budget(&self) -> u64 {
        self.budget.get()
    }

    pub fn used(&self) -> u64 {
        self.used.get()
    }

    pub fn peak(&self) -> u64 {
        self.peak.get()
    }

    pub fn remaining(&self) -> u64 {
        self.budget.get().saturating_sub(self.used.get())
    }

    /// Reserve `bytes`. On failure nothing is charged, here or in any parent.
    pub fn charge(&self, bytes: u64) -> Result<(), OverBudget> {
        let used = self.used.get();
        let next = used.saturating_add(bytes);
        if next > self.budget.get() {
            return Err(OverBudget {
                requested: bytes,
                used,
                budget: self.budget.get(),
            });
        }

        if let Some(parent) = &self.parent {
            // If the parent refuses, this accountant must not record the
            // charge either.
            parent.charge(bytes)?;
        }

        self.used.set(next);
        if next > self.peak.get() {
            self.peak.set(next);
        }
        Ok(())
    }

    pub fn release(&self, bytes: u64) {
        let released = bytes.min(self.used.get());
        self.used.set(self.used.get() - released);
        if let Some(parent) = &self.parent {
            parent.release(released);
        }
    }

    /// Release everything this accountant holds.
    pub fn reset(&self) {
        self.release(self.used.get());
    }
}
