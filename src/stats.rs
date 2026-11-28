//! Database properties and statistics.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Maximum number of levels supported for read-level sampling.
pub(crate) const MAX_LEVELS: usize = 32;

/// Number of cache-line-aligned stripes for hot reader counters.
/// Must be a power of two.
const READER_SHARDS: usize = 64;

static NEXT_READER_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static READER_SHARD_ID: usize =
        NEXT_READER_SHARD.fetch_add(1, Ordering::Relaxed) & (READER_SHARDS - 1);
}

#[inline]
fn reader_shard_index() -> usize {
    READER_SHARD_ID.try_with(|&idx| idx).unwrap_or(0)
}

#[repr(align(64))]
struct ReaderStatsShard {
    bytes_read: AtomicU64,
    block_cache_hits: AtomicU64,
    block_cache_misses: AtomicU64,
    read_sample_counter: AtomicU64,
}

impl ReaderStatsShard {
    fn new() -> Self {
        Self {
            bytes_read: AtomicU64::new(0),
            block_cache_hits: AtomicU64::new(0),
            block_cache_misses: AtomicU64::new(0),
            read_sample_counter: AtomicU64::new(0),
        }
    }
}

/// Tracks database statistics for monitoring and diagnostics.
///
/// Writer-hot, reader-hot, and background counters are separated by 64-byte
/// alignment/padding, and reader-hot counters are sharded across cache lines
/// so concurrent reads never bounce a shared line across cores.
#[repr(C)]
pub struct DbStats {
    #[cfg(feature = "bench-metrics")]
    pub(crate) l0_profile: parking_lot::Mutex<crate::compaction::profile::L0CompactionProfile>,
    /// Total bytes written by the user.
    pub bytes_written: AtomicU64,
    _pad_write: [u8; 56],
    /// Sharded reader-hot counters (`bytes_read`, block-cache hits/misses,
    /// and per-stripe read-level sampling counters).
    reader_shards: [ReaderStatsShard; READER_SHARDS],
    /// Number of compactions completed.
    pub compactions_completed: AtomicU64,
    /// Total bytes written during compaction (write amplification numerator).
    pub compaction_bytes_written: AtomicU64,
    /// Number of flushes completed.
    pub flushes_completed: AtomicU64,
    /// DB-wide count of sampled L2+ reads (1 per 16 per-stripe reads);
    /// every 64 samples (~1,024 L2+ reads DB-wide) triggers a read-compaction check.
    read_compaction_samples: AtomicU64,
    /// Per-level read sample counters.
    pub read_level_samples: [AtomicU64; MAX_LEVELS],
}

impl DbStats {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "bench-metrics")]
            l0_profile: Default::default(),
            bytes_written: AtomicU64::new(0),
            _pad_write: [0; 56],
            reader_shards: std::array::from_fn(|_| ReaderStatsShard::new()),
            compactions_completed: AtomicU64::new(0),
            compaction_bytes_written: AtomicU64::new(0),
            flushes_completed: AtomicU64::new(0),
            read_compaction_samples: AtomicU64::new(0),
            read_level_samples: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    #[inline]
    fn reader_shard(&self) -> &ReaderStatsShard {
        &self.reader_shards[reader_shard_index()]
    }

    pub fn record_write(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_read(&self, bytes: u64) {
        self.reader_shard()
            .bytes_read
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Total key and value bytes returned by point lookups (`get*`); iterator
    /// reads are not counted.
    pub fn bytes_read(&self) -> u64 {
        self.reader_shards.iter().fold(0u64, |acc, s| {
            acc.wrapping_add(s.bytes_read.load(Ordering::Relaxed))
        })
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

    #[inline]
    pub fn record_cache_hit(&self) {
        self.reader_shard()
            .block_cache_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_cache_miss(&self) {
        self.reader_shard()
            .block_cache_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Total number of block cache hits across all reader stripes.
    pub fn block_cache_hits(&self) -> u64 {
        self.reader_shards.iter().fold(0u64, |acc, s| {
            acc.wrapping_add(s.block_cache_hits.load(Ordering::Relaxed))
        })
    }

    /// Total number of block cache misses across all reader stripes.
    pub fn block_cache_misses(&self) -> u64 {
        self.reader_shards.iter().fold(0u64, |acc, s| {
            acc.wrapping_add(s.block_cache_misses.load(Ordering::Relaxed))
        })
    }

    /// Sample an L2+ read at `level` and report whether to check read-triggered
    /// compaction.
    ///
    /// Fifteen out of sixteen reads touch only the caller's reader stripe. On
    /// every 16th read per stripe (`count % 16 == 0`), records a sample for
    /// `level` and advances the DB-wide sample counter, returning `true` every
    /// 64th DB-wide sample (~1,024 L2+ reads DB-wide, starting with the first
    /// sampled L2+ read) without firing a startup burst across stripes.
    #[inline]
    pub fn record_l2_read_sample(&self, level: usize) -> bool {
        let count = self
            .reader_shard()
            .read_sample_counter
            .fetch_add(1, Ordering::Relaxed);
        if !count.is_multiple_of(16) {
            return false;
        }
        if level < self.read_level_samples.len() {
            self.read_level_samples[level].fetch_add(1, Ordering::Relaxed);
        }
        let samples = self.read_compaction_samples.fetch_add(1, Ordering::Relaxed);
        samples.is_multiple_of(64)
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
    use std::sync::Arc;

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

        stats.record_read(42);
        stats.record_cache_hit();
        stats.record_cache_miss();
        assert_eq!(stats.bytes_read(), 42);
        assert_eq!(stats.block_cache_hits(), 1);
        assert_eq!(stats.block_cache_misses(), 1);
    }

    #[test]
    fn test_sharded_reader_stats_multithreaded_exact_sum() {
        let stats = Arc::new(DbStats::new());
        let threads = 16usize;
        let iters = 1_000u64;
        std::thread::scope(|s| {
            for t in 0..threads {
                let stats = Arc::clone(&stats);
                s.spawn(move || {
                    for i in 0..iters {
                        stats.record_read(t as u64 + 1);
                        if i % 3 == 0 {
                            stats.record_cache_miss();
                        } else {
                            stats.record_cache_hit();
                        }
                    }
                });
            }
        });
        let expected_bytes: u64 = (1..=threads as u64).sum::<u64>() * iters;
        let expected_misses = threads as u64 * iters.div_ceil(3);
        let expected_hits = threads as u64 * iters - expected_misses;
        assert_eq!(stats.bytes_read(), expected_bytes);
        assert_eq!(stats.block_cache_misses(), expected_misses);
        assert_eq!(stats.block_cache_hits(), expected_hits);
    }

    #[test]
    fn test_l2_read_sampling_preserves_db_wide_cadence_without_startup_burst() {
        let stats = Arc::new(DbStats::new());
        let triggers = Arc::new(AtomicU64::new(0));
        // 8 threads * 128 reads = 1,024 L2+ reads DB-wide -> 64 samples -> exactly 1 trigger.
        std::thread::scope(|s| {
            for _ in 0..8 {
                let stats = Arc::clone(&stats);
                let triggers = Arc::clone(&triggers);
                s.spawn(move || {
                    for _ in 0..128 {
                        if stats.record_l2_read_sample(2) {
                            triggers.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(triggers.load(Ordering::Relaxed), 1);
        let samples = stats.take_read_level_samples();
        assert_eq!(samples[2], 64);
    }
}
