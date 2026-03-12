use super::policy::ReplacementPolicy;
use super::frame::BufferFrame;
use std::collections::HashMap;

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

    fn victim(&mut self, frames: &Vec<BufferFrame>) -> Option<usize> {

        let mut victim_index = None;
        let mut oldest_time = u64::MAX;

        for (i, frame) in frames.iter().enumerate() {

            if frame.metadata.pin_count != 0 {
                continue;
            }

            let time = *self.timestamps.get(&i).unwrap_or(&0);

            if time < oldest_time {
                oldest_time = time;
                victim_index = Some(i);
            }
        }

        victim_index
    }

    fn record_access(&mut self, frame_id: usize) {
        self.current_time += 1;
        self.timestamps.insert(frame_id, self.current_time);
    }
}