pub struct BufferStats {
    pub hit_count: u64,
    pub miss_count: u64,
    pub eviction_count: u64,
    pub dirty_flush_count: u64,
}

impl BufferStats {
    pub fn new() -> Self {
        Self {
            hit_count: 0,
            miss_count: 0,
            eviction_count: 0,
            dirty_flush_count: 0,
        }
    }

    pub fn record_hit(&mut self) {
        self.hit_count += 1;
    }

    pub fn record_miss(&mut self) {
        self.miss_count += 1;
    }

    pub fn record_eviction(&mut self) {
        self.eviction_count += 1;
    }

    pub fn record_dirty_flush(&mut self) {
        self.dirty_flush_count += 1;
    }

    pub fn hit_ratio(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            self.hit_count as f64 / total as f64
        }
    }
}