//! Database properties and statistics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Maximum number of levels supported for read-level sampling.
pub(crate) const MAX_LEVELS: usize = 32;

/// Tracks database statistics for monitoring and diagnostics.
pub struct DbStats {
    /// Total bytes written by the user.
    pub bytes_written: AtomicU64,
    /// Total bytes read by the user.
    pub bytes_read: AtomicU64,
    /// Number of compactions completed.
    pub compactions_completed: AtomicU64,
    /// Total bytes written during compaction (write amplification numerator).
    pub compaction_bytes_written: AtomicU64,
    /// Number of flushes completed.
    pub flushes_completed: AtomicU64,
    /// Number of block cache hits.
    pub block_cache_hits: AtomicU64,
    /// Number of block cache misses.
    pub block_cache_misses: AtomicU64,
    /// Per-level read sample counters.
    pub read_level_samples: [AtomicU64; MAX_LEVELS],
    /// Counter for sampling reads (only sample every Nth read).
    pub read_sample_counter: AtomicU64,
}

impl DbStats {
    pub fn new() -> Self {
        Self {
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            compactions_completed: AtomicU64::new(0),
            compaction_bytes_written: AtomicU64::new(0),
            flushes_completed: AtomicU64::new(0),
            block_cache_hits: AtomicU64::new(0),
            block_cache_misses: AtomicU64::new(0),
            read_level_samples: std::array::from_fn(|_| AtomicU64::new(0)),
            read_sample_counter: AtomicU64::new(0),
        }
    }

    pub fn record_write(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_compaction_bytes(&self, bytes_written: u64) {
        self.compaction_bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
    }

    pub fn record_compaction_completed(&self) {
        self.compactions_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_flush(&self) {
        self.flushes_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_hit(&self) {
        self.block_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.block_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Sample a read at the given level. Only records every 16th read to
    /// reduce contention on the atomic counters.
    pub fn maybe_sample_read_level(&self, level: usize) {
        let count = self.read_sample_counter.fetch_add(1, Ordering::Relaxed);
        // Sample every 16 reads to reduce atomic contention.
        if count.is_multiple_of(16) && level < self.read_level_samples.len() {
            self.read_level_samples[level].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Atomically take (read and reset) the per-level read samples.
    pub fn take_read_level_samples(&self) -> [u64; MAX_LEVELS] {
        let mut result = [0u64; MAX_LEVELS];
        for (i, slot) in result.iter_mut().enumerate() {
            *slot = self.read_level_samples[i].swap(0, Ordering::Relaxed);
        }
        result
    }
}

impl Default for DbStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_basic() {
        let stats = DbStats::new();
        stats.record_write(100);
        stats.record_write(200);
        assert_eq!(stats.bytes_written.load(Ordering::Relaxed), 300);

        stats.record_compaction_bytes(1000);
        stats.record_compaction_completed();
        assert_eq!(stats.compactions_completed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.compaction_bytes_written.load(Ordering::Relaxed), 1000);
    }
}
