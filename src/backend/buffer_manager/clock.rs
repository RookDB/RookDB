use super::policy::ReplacementPolicy;
use super::frame::BufferFrame;
use crate::backend::buffer_manager::{RESERVED_FRAMES, BUFFER_SIZE, PAGE_SIZE};

pub struct ClockPolicy {
    pub hand: usize,
}

impl ClockPolicy {
    pub fn new() -> Self {
        Self { hand: RESERVED_FRAMES }
    }
}

impl ReplacementPolicy for ClockPolicy {

    fn victim(&mut self, frames: &mut Vec<BufferFrame>) -> Option<usize> {

        let start = RESERVED_FRAMES;
        let end = frames.len();

        if start >= end {
            return None;
        }

        let mut scanned = 0;
        let max_scan = 2 * (end - start + 1);

        while scanned < max_scan {

            // Ensure hand stays in valid range
            if self.hand < start || self.hand >= end {
                self.hand = start;
            }

            let frame = &mut frames[self.hand];

            if frame.metadata.pin_count == 0 {

                if frame.metadata.usage_count == 0 {
                    let victim = self.hand;

                    self.hand += 1;
                    if self.hand >= end {
                        self.hand = start;
                    }

                    return Some(victim);
                } else {
                    // second chance
                    frame.metadata.usage_count = 0;
                }
            }

            self.hand += 1;
            if self.hand >= end {
                self.hand = start;
            }

            scanned += 1;
        }

        None // all frames pinned
    }

    fn record_access(&mut self, _frame_id: usize) {
        // handled via usage_count in BufferPool
    }
}