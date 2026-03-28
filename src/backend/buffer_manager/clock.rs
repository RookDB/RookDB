use super::policy::ReplacementPolicy;
use super::frame::BufferFrame;

pub struct ClockPolicy {
    pub hand: usize,
}

impl ClockPolicy {
    pub fn new() -> Self {
        Self { hand: 0 }
    }
}

impl ReplacementPolicy for ClockPolicy {

    fn victim(&mut self, frames: &mut Vec<BufferFrame>) -> Option<usize> {

        let n = frames.len();
        let mut scanned = 0;

        while scanned < 2 * n {

            let frame = &mut frames[self.hand];

            if frame.metadata.pin_count == 0 {

                if frame.metadata.usage_count == 0 {
                    let victim = self.hand;
                    self.hand = (self.hand + 1) % n;
                    return Some(victim);
                } else {
                    // give second chance
                    frame.metadata.usage_count = 0;
                }
            }

            self.hand = (self.hand + 1) % n;
            scanned += 1;
        }

        None // all pages pinned
    }

    fn record_access(&mut self, _frame_id: usize) {
        // handled via usage_count in BufferPool
    }
}