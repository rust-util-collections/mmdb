//! Database properties and statistics.

use std::sync::atomic::{AtomicU64, Ordering};

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
        }
    }

    pub fn record_write(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_compaction(&self, bytes_written: u64) {
        self.compactions_completed.fetch_add(1, Ordering::Relaxed);
        self.compaction_bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
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

        stats.record_compaction(1000);
        assert_eq!(stats.compactions_completed.load(Ordering::Relaxed), 1);
    }
}
