// FSM (Free Space Map) module
// Implements a 3-level binary max-tree for efficient free-space management
// across heap pages. This module provides O(log N) search for pages with
// sufficient free space and O(1) early-exit when table is full.

pub mod fsm;

pub use fsm::{FSM, FSMPage, FSM_NODES_PER_PAGE, FSM_SLOTS_PER_PAGE, 
              FSM_LEVELS, FSM_PAGE_SIZE};
