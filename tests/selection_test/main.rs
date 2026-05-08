// ============================================================================
// 0_surjit/selection_test/main.rs
//
// Root of the `selection_suite` cargo test target.
// Registered in Cargo.toml as:
//   [[test]]
//   name = "selection_suite"
//   path = "0_surjit/selection_test/main.rs"
//
// Run with:
//   cargo test --test selection_suite
//
// Run a specific sub-module:
//   cargo test --test selection_suite -- test_basic
//   cargo test --test selection_suite -- test_null_logic
//   cargo test --test selection_suite -- test_arithmetic
//   cargo test --test selection_suite -- test_in_like
//   cargo test --test selection_suite -- test_short_circuit
//   cargo test --test selection_suite -- test_varlen
//   cargo test --test selection_suite -- test_streaming
//
// All files live ONLY inside 0_surjit/selection_test/ — nothing is written
// to the tests/ directory.
// ============================================================================

mod test_basic;
mod test_null_logic;
mod test_arithmetic;
mod test_in_like;
mod test_short_circuit;
mod test_varlen;
mod test_streaming;
