use super::policy::ReplacementPolicy;
use super::frame::BufferFrame;
use std::collections::HashMap;

pub struct LRUKPolicy {
    k: usize,
    current_time: u64,
    history: HashMap<usize, Vec<u64>>, // frame_id -> access timestamps
}

impl LRUKPolicy {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            current_time: 0,
            history: HashMap::new(),
        }
    }

    fn backward_k_distance(&self, frame_id: usize) -> u64 {
        match self.history.get(&frame_id) {
            Some(timestamps) => {
                if timestamps.len() < self.k {
                    // Not enough accesses → treat as infinite distance
                    u64::MAX
                } else {
                    let kth_time = timestamps[0]; // oldest among last K
                    self.current_time - kth_time
                }
            }
            None => u64::MAX,
        }
    }
}

impl ReplacementPolicy for LRUKPolicy {

    fn victim(&mut self, frames: &mut Vec<BufferFrame>) -> Option<usize> {

        let mut victim_index = None;
        let mut max_distance = 0;

        for (i, frame) in frames.iter().enumerate() {

            // Skip pinned frames
            if frame.metadata.pin_count != 0 {
                continue;
            }

            let distance = self.backward_k_distance(i);

            if victim_index.is_none() || distance > max_distance {
                max_distance = distance;
                victim_index = Some(i);
            }
        }

        victim_index
    }

    fn record_access(&mut self, frame_id: usize) {
        self.current_time += 1;

        let entry = self.history.entry(frame_id).or_insert(Vec::new());
        entry.push(self.current_time);

        // Keep only last K accesses
        if entry.len() > self.k {
            entry.remove(0);
        }
    }
}