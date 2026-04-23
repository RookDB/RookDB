use super::policy::ReplacementPolicy;
use super::frame::BufferFrame;
use std::collections::HashMap;
use crate::backend::buffer_manager::{RESERVED_FRAMES, BUFFER_SIZE, PAGE_SIZE};

pub struct LRUPolicy {
    timestamps: HashMap<usize, u64>,
    current_time: u64,
}

impl LRUPolicy {
    pub fn new() -> Self {
        Self {
            timestamps: HashMap::new(),
            current_time: 0,
        }
    }
}

impl ReplacementPolicy for LRUPolicy {

    fn victim(&mut self, frames: &mut Vec<BufferFrame>) -> Option<usize> {
    let mut victim_index = None;
    let mut oldest_time = u64::MAX;

    for i in RESERVED_FRAMES..frames.len() {
        let frame = &frames[i];

        if frame.metadata.pin_count != 0 {
            continue;
        }

        // Convert to logical index
        let logical_id = i - RESERVED_FRAMES;

        let time = self.timestamps.get(&logical_id).copied().unwrap_or(0);

        if time < oldest_time {
            oldest_time = time;
            victim_index = Some(i); // return physical index
        }
    }

    victim_index
}
    fn record_access(&mut self, frame_id: usize) {
        self.current_time += 1;
        self.timestamps.insert(frame_id, self.current_time);
    }
}