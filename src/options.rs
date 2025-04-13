//! Configuration options for MMDB.

use crate::sst::format::CompressionType;

/// Database-level options.
#[derive(Debug)]
pub struct DbOptions {
    /// Create the database directory if it does not exist.
    pub create_if_missing: bool,
    /// Return an error if the database already exists.
    pub error_if_exists: bool,
    /// Size of a single MemTable in bytes before it is frozen.
    pub write_buffer_size: usize,
    /// Maximum number of immutable MemTables waiting to be flushed.
    pub max_immutable_memtables: usize,
    /// Number of L0 files that triggers compaction.
    pub l0_compaction_trigger: usize,
    /// Target size for SST files in bytes.
    pub target_file_size_base: u64,
    /// Max total size of L1 in bytes.
    pub max_bytes_for_level_base: u64,
    /// Multiplier between levels.
    pub max_bytes_for_level_multiplier: f64,
    /// Maximum number of levels (including L0).
    pub num_levels: usize,
    /// Block size for SST data blocks.
    pub block_size: usize,
    /// Restart interval for prefix compression in data blocks.
    pub block_restart_interval: usize,
    /// Bits per key for bloom filter. 0 disables bloom filter.
    pub bloom_bits_per_key: u32,
    /// Compression type for SST data blocks.
    pub compression: CompressionType,
    /// Block cache capacity in bytes. 0 disables caching.
    pub block_cache_capacity: u64,
    /// Maximum number of open SST files cached.
    pub max_open_files: u64,
    /// Number of L0 files that triggers write slowdown.
    pub l0_slowdown_trigger: usize,
    /// Number of L0 files that stops writes until compaction completes.
    pub l0_stop_trigger: usize,
    /// Optional rate limiter for compaction writes (bytes/sec). 0 = no limit.
    pub rate_limiter_bytes_per_sec: u64,
    /// Optional compaction filter.
    pub compaction_filter: Option<Box<dyn CompactionFilter>>,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            error_if_exists: false,
            write_buffer_size: 64 * 1024 * 1024, // 64 MB
            max_immutable_memtables: 4,
            l0_compaction_trigger: 4,
            target_file_size_base: 64 * 1024 * 1024, // 64 MB
            max_bytes_for_level_base: 256 * 1024 * 1024, // 256 MB
            max_bytes_for_level_multiplier: 10.0,
            num_levels: 7,
            block_size: 4096,
            block_restart_interval: 16,
            bloom_bits_per_key: 10,
            compression: CompressionType::None,
            block_cache_capacity: 64 * 1024 * 1024, // 64 MB
            max_open_files: 1000,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
            rate_limiter_bytes_per_sec: 0,
            compaction_filter: None,
        }
    }
}

impl Clone for DbOptions {
    fn clone(&self) -> Self {
        Self {
            create_if_missing: self.create_if_missing,
            error_if_exists: self.error_if_exists,
            write_buffer_size: self.write_buffer_size,
            max_immutable_memtables: self.max_immutable_memtables,
            l0_compaction_trigger: self.l0_compaction_trigger,
            target_file_size_base: self.target_file_size_base,
            max_bytes_for_level_base: self.max_bytes_for_level_base,
            max_bytes_for_level_multiplier: self.max_bytes_for_level_multiplier,
            num_levels: self.num_levels,
            block_size: self.block_size,
            block_restart_interval: self.block_restart_interval,
            bloom_bits_per_key: self.bloom_bits_per_key,
            compression: self.compression,
            block_cache_capacity: self.block_cache_capacity,
            max_open_files: self.max_open_files,
            l0_slowdown_trigger: self.l0_slowdown_trigger,
            l0_stop_trigger: self.l0_stop_trigger,
            rate_limiter_bytes_per_sec: self.rate_limiter_bytes_per_sec,
            compaction_filter: None, // Cannot clone trait objects
        }
    }
}

/// Preset profiles for common workloads.
impl DbOptions {
    /// Balanced profile — good for mixed read/write workloads.
    pub fn balanced() -> Self {
        Self::default()
    }

    /// Write-heavy profile — optimized for high write throughput.
    pub fn write_heavy() -> Self {
        Self {
            write_buffer_size: 128 * 1024 * 1024, // 128 MB
            l0_compaction_trigger: 8,
            compression: CompressionType::Lz4,
            ..Default::default()
        }
    }

    /// Read-heavy profile — optimized for read latency.
    pub fn read_heavy() -> Self {
        Self {
            write_buffer_size: 32 * 1024 * 1024, // 32 MB
            l0_compaction_trigger: 2,
            block_cache_capacity: 256 * 1024 * 1024, // 256 MB
            bloom_bits_per_key: 14,
            ..Default::default()
        }
    }
}

/// Options for read operations.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// If set, reads will use this snapshot sequence number.
    pub snapshot: Option<u64>,
    /// Whether to fill the block cache for this read. Default: true.
    pub fill_cache: bool,
    /// Whether to verify checksums on reads. Default: false.
    pub verify_checksums: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            snapshot: None,
            fill_cache: true,
            verify_checksums: false,
        }
    }
}

/// Options for write operations.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// If true, fsync the WAL before acknowledging the write.
    pub sync: bool,
    /// If true, skip writing to the WAL (data may be lost on crash).
    pub disable_wal: bool,
    /// If true, return an error instead of sleeping when writes are throttled.
    pub no_slowdown: bool,
    /// If true, gives this write lower priority during contention.
    pub low_pri: bool,
}

/// Decision returned by a compaction filter.
#[derive(Debug)]
pub enum CompactionFilterDecision {
    /// Keep the key-value pair.
    Keep,
    /// Remove the key-value pair.
    Remove,
    /// Change the value.
    ChangeValue(Vec<u8>),
}

/// Trait for custom compaction filters.
///
/// During compaction, each key-value pair is passed to the filter for a decision.
pub trait CompactionFilter: Send + Sync {
    fn filter(&self, level: usize, key: &[u8], value: &[u8]) -> CompactionFilterDecision;
}

impl std::fmt::Debug for dyn CompactionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompactionFilter")
    }
}
