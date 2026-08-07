//! Memory accounting.
//!
//! The accountant is a budget, not a measurement - Rust exposes no allocator
//! introspection - so what matters is that it is monotone, that it never
//! under-reports, and that a refused charge leaves no trace anywhere in the
//! tree.

use storage_manager::join::memory::{HASH_ENTRY_OVERHEAD, row_footprint};
use storage_manager::join::{JoinConfig, MemoryAccountant};

#[test]
fn charges_and_releases_track_usage() {
    let accountant = MemoryAccountant::new(1000);
    assert_eq!(accountant.used(), 0);
    assert_eq!(accountant.remaining(), 1000);

    accountant.charge(400).expect("fits");
    assert_eq!(accountant.used(), 400);
    assert_eq!(accountant.remaining(), 600);

    accountant.charge(600).expect("exactly fills the budget");
    assert_eq!(accountant.used(), 1000);
    assert_eq!(accountant.remaining(), 0);

    accountant.release(1000);
    assert_eq!(accountant.used(), 0);
}

#[test]
fn an_over_budget_charge_is_refused_and_records_nothing() {
    let accountant = MemoryAccountant::new(100);
    accountant.charge(90).expect("fits");

    let refused = accountant.charge(20).expect_err("20 more would exceed 100");
    assert_eq!(refused.requested, 20);
    assert_eq!(refused.used, 90);
    assert_eq!(refused.budget, 100);

    assert_eq!(
        accountant.used(),
        90,
        "a refused charge must not be recorded"
    );
    assert!(refused.to_string().contains("100"), "{refused}");
}

/// Peak is what EXPLAIN ANALYZE reports, so it must survive a release.
#[test]
fn peak_records_the_high_water_mark() {
    let accountant = MemoryAccountant::new(1000);
    accountant.charge(700).expect("fits");
    assert_eq!(accountant.peak(), 700);

    accountant.release(700);
    accountant.charge(100).expect("fits");

    assert_eq!(accountant.used(), 100);
    assert_eq!(accountant.peak(), 700, "peak must not fall back");
}

/// A child draws on its parent, so nested structures cannot collectively
/// exceed the operator's allowance.
#[test]
fn a_child_charges_against_its_parent() {
    let parent = MemoryAccountant::new(1000);
    let child = MemoryAccountant::child(&parent, 800);

    child.charge(500).expect("fits both budgets");
    assert_eq!(child.used(), 500);
    assert_eq!(parent.used(), 500, "the parent sees its child's usage");

    child.release(500);
    assert_eq!(parent.used(), 0);
}

/// If the parent refuses, the child must not record the charge either -
/// otherwise the two would drift apart and the child would refuse work it
/// could actually do.
#[test]
fn a_parent_refusal_rolls_the_child_back() {
    let parent = MemoryAccountant::new(100);
    let child = MemoryAccountant::child(&parent, 1000);

    parent.charge(80).expect("fits");

    let refused = child
        .charge(50)
        .expect_err("the child's own budget allows it, but the parent's does not");
    assert_eq!(refused.budget, 100, "the parent's budget is what refused");

    assert_eq!(child.used(), 0, "the child must record nothing");
    assert_eq!(parent.used(), 80, "the parent is unchanged");

    // And the child can still spend what genuinely remains.
    child.charge(20).expect("20 still fits");
    assert_eq!(parent.used(), 100);
}

#[test]
fn reset_releases_everything_including_from_the_parent() {
    let parent = MemoryAccountant::new(1000);
    let child = MemoryAccountant::child(&parent, 1000);

    child.charge(300).expect("fits");
    child.charge(200).expect("fits");
    child.reset();

    assert_eq!(child.used(), 0);
    assert_eq!(parent.used(), 0);
}

/// Releasing more than is held must not underflow.
#[test]
fn over_release_saturates() {
    let accountant = MemoryAccountant::new(1000);
    accountant.charge(10).expect("fits");
    accountant.release(999_999);
    assert_eq!(accountant.used(), 0);
}

/// The footprint of a row must exceed the row itself, or the budget would
/// under-count every allocation the operator makes.
#[test]
fn footprints_account_for_more_than_the_payload() {
    assert!(row_footprint(0) > 0, "even an empty row costs something");
    assert!(row_footprint(100) > 100);
    assert_eq!(
        row_footprint(200) - row_footprint(100),
        100,
        "the overhead must be constant per row"
    );
    assert!(HASH_ENTRY_OVERHEAD > 0);
}

// ── Budget resolution ────────────────────────────────────────────────────────

/// An explicit budget is clamped into the supported range rather than
/// accepted blindly.
#[test]
fn explicit_budgets_are_clamped() {
    use storage_manager::join::config::{MAX_WORK_MEMORY, MIN_WORK_MEMORY};

    assert_eq!(
        JoinConfig::with_work_memory(1).work_memory_bytes,
        MIN_WORK_MEMORY
    );
    assert_eq!(
        JoinConfig::with_work_memory(u64::MAX).work_memory_bytes,
        MAX_WORK_MEMORY
    );

    let sensible = 8 * 1024 * 1024;
    assert_eq!(
        JoinConfig::with_work_memory(sensible).work_memory_bytes,
        sensible
    );
}

/// The derived default must be usable without any configuration.
#[test]
fn the_resolved_default_is_within_bounds() {
    use storage_manager::join::config::{MAX_WORK_MEMORY, MIN_WORK_MEMORY};

    let config = JoinConfig::resolve();
    assert!(config.work_memory_bytes >= MIN_WORK_MEMORY);
    assert!(config.work_memory_bytes <= MAX_WORK_MEMORY);
    assert!(
        config.spill_root.ends_with("join"),
        "spill root should be namespaced: {:?}",
        config.spill_root
    );
}
