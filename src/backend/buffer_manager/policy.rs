use super::frame::BufferFrame;

pub trait ReplacementPolicy {
    // Select a victim frame for eviction
    fn victim(&mut self, frames: &Vec<BufferFrame>) -> Option<usize>;

    // Called whenever a frame is accessed
    fn record_access(&mut self, frame_id: usize);
}