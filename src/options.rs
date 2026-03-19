//! Configuration options for MMDB.

use std::sync::Arc;

use crate::sst::format::CompressionType;

/// Database-level options.
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
    /// Fixed prefix length for prefix bloom filter. 0 = disabled (default).
    /// When set, each SST file stores a bloom filter of key prefixes,
    /// allowing `iter_with_prefix()` to skip entire SST files.
    pub prefix_len: usize,
    /// Per-level compression types. If empty, uses `compression` for all levels.
    /// Index corresponds to level number (0 = L0, 1 = L1, etc.).
    pub compression_per_level: Vec<CompressionType>,
    /// Optional compaction filter.
    /// Optional compaction filter. Wrapped in `Arc` so it survives Clone
    /// and is shared with background compaction threads.
    pub compaction_filter: Option<Arc<dyn CompactionFilter>>,

    // ---- Compaction parallelism (RocksDB: increase_parallelism) ----
    /// Maximum number of background compaction threads. Default: 1.
    /// RocksDB equivalent: `max_background_compactions` / `increase_parallelism`.
    pub max_background_compactions: usize,
    /// Maximum sub-compactions per compaction job. Default: 1 (no sub-compaction).
    /// RocksDB equivalent: `max_subcompactions`.
    pub max_subcompactions: usize,

    // ---- Cache behavior ----
    /// Pin L0 index and filter blocks in block cache (never evict). Default: true.
    /// RocksDB equivalent: `pin_l0_filter_and_index_blocks_in_cache`.
    pub pin_l0_filter_and_index_blocks_in_cache: bool,
    /// Cache index and filter blocks in block cache. Default: true.
    /// RocksDB equivalent: `cache_index_and_filter_blocks`.
    pub cache_index_and_filter_blocks: bool,

    // ---- Write buffer ----
    /// Maximum total number of write buffers (active + immutable). Default: 6.
    /// RocksDB equivalent: `max_write_buffer_number`.
    pub max_write_buffer_number: usize,

    // ---- Advanced tuning (reserved, documented) ----
    /// Use dynamic level sizes for compaction. Default: false.
    /// RocksDB equivalent: `level_compaction_dynamic_level_bytes`.
    pub level_compaction_dynamic_level_bytes: bool,
    /// Allow concurrent memtable writes from multiple threads. Default: false.
    /// RocksDB equivalent: `allow_concurrent_memtable_write`.
    pub allow_concurrent_memtable_write: bool,
    /// Memtable prefix bloom ratio (fraction of memtable for bloom). Default: 0.0 (disabled).
    /// RocksDB equivalent: `memtable_prefix_bloom_ratio`.
    pub memtable_prefix_bloom_ratio: f64,
    /// Factories for block property collectors. Each factory is called once per SST
    /// file build to produce a fresh collector instance.
    pub block_property_collectors: Vec<Arc<dyn Fn() -> Box<dyn BlockPropertyCollector> + Send + Sync>>,
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
            prefix_len: 0,
            compression_per_level: Vec::new(),
            compaction_filter: None,
            max_background_compactions: 1,
            max_subcompactions: 1,
            pin_l0_filter_and_index_blocks_in_cache: true,
            cache_index_and_filter_blocks: true,
            max_write_buffer_number: 6,
            level_compaction_dynamic_level_bytes: false,
            allow_concurrent_memtable_write: false,
            memtable_prefix_bloom_ratio: 0.0,
            block_property_collectors: Vec::new(),
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
            prefix_len: self.prefix_len,
            compression_per_level: self.compression_per_level.clone(),
            compaction_filter: self.compaction_filter.clone(),
            max_background_compactions: self.max_background_compactions,
            max_subcompactions: self.max_subcompactions,
            pin_l0_filter_and_index_blocks_in_cache: self.pin_l0_filter_and_index_blocks_in_cache,
            cache_index_and_filter_blocks: self.cache_index_and_filter_blocks,
            max_write_buffer_number: self.max_write_buffer_number,
            level_compaction_dynamic_level_bytes: self.level_compaction_dynamic_level_bytes,
            allow_concurrent_memtable_write: self.allow_concurrent_memtable_write,
            memtable_prefix_bloom_ratio: self.memtable_prefix_bloom_ratio,
            block_property_collectors: self.block_property_collectors.clone(),
        }
    }
}

impl std::fmt::Debug for DbOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbOptions")
            .field("create_if_missing", &self.create_if_missing)
            .field("error_if_exists", &self.error_if_exists)
            .field("write_buffer_size", &self.write_buffer_size)
            .field("max_immutable_memtables", &self.max_immutable_memtables)
            .field("l0_compaction_trigger", &self.l0_compaction_trigger)
            .field("target_file_size_base", &self.target_file_size_base)
            .field("max_bytes_for_level_base", &self.max_bytes_for_level_base)
            .field("max_bytes_for_level_multiplier", &self.max_bytes_for_level_multiplier)
            .field("num_levels", &self.num_levels)
            .field("block_size", &self.block_size)
            .field("block_restart_interval", &self.block_restart_interval)
            .field("bloom_bits_per_key", &self.bloom_bits_per_key)
            .field("compression", &self.compression)
            .field("block_cache_capacity", &self.block_cache_capacity)
            .field("max_open_files", &self.max_open_files)
            .field("prefix_len", &self.prefix_len)
            .field("block_property_collectors", &self.block_property_collectors.len())
            .finish()
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
            l0_slowdown_trigger: 20,
            l0_stop_trigger: 36,
            compression: CompressionType::Lz4,
            max_background_compactions: 4,
            max_write_buffer_number: 8,
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
            pin_l0_filter_and_index_blocks_in_cache: true,
            ..Default::default()
        }
    }
}

/// Options for read operations.
#[derive(Clone)]
pub struct ReadOptions {
    /// If set, reads will use this snapshot sequence number.
    pub snapshot: Option<u64>,
    /// Whether to fill the block cache for this read. Default: true.
    pub fill_cache: bool,
    /// Whether to verify checksums on reads. Default: false.
    pub verify_checksums: bool,
    /// Readahead size hint in bytes for sequential iteration. 0 = auto. Default: 0.
    /// RocksDB equivalent: `readahead_size`.
    pub readahead_size: usize,
    /// If true, ignore prefix bloom filters and do a total order seek. Default: false.
    /// RocksDB equivalent: `total_order_seek`.
    pub total_order_seek: bool,
    /// If true, pin data blocks in memory during iteration. Default: false.
    /// RocksDB equivalent: `pin_data`.
    pub pin_data: bool,
    /// Optional callback checked during iteration. If it returns `true` for a
    /// user key, that key is skipped without being yielded.
    pub skip_point: Option<Arc<dyn Fn(&[u8]) -> bool + Send + Sync>>,
    /// Block property filters to skip entire data blocks during iteration.
    pub block_property_filters: Vec<Arc<dyn BlockPropertyFilter>>,
}

impl std::fmt::Debug for ReadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOptions")
            .field("snapshot", &self.snapshot)
            .field("fill_cache", &self.fill_cache)
            .field("verify_checksums", &self.verify_checksums)
            .field("readahead_size", &self.readahead_size)
            .field("total_order_seek", &self.total_order_seek)
            .field("pin_data", &self.pin_data)
            .field("skip_point", &self.skip_point.as_ref().map(|_| ".."))
            .field("block_property_filters", &self.block_property_filters.len())
            .finish()
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            snapshot: None,
            fill_cache: true,
            verify_checksums: false,
            readahead_size: 0,
            total_order_seek: false,
            pin_data: false,
            skip_point: None,
            block_property_filters: Vec::new(),
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

/// Collects properties from key-value pairs during SST building.
/// One instance per property type per SST file build.
pub trait BlockPropertyCollector: Send + Sync {
    /// Called for each key-value pair added to the current data block.
    fn add(&mut self, key: &[u8], value: &[u8]);
    /// Called when a data block is flushed. Returns serialized properties
    /// for this block. The collector is then reset for the next block.
    fn finish_block(&mut self) -> Vec<u8>;
    /// Unique name identifying this collector type.
    fn name(&self) -> &str;
}

/// Filters data blocks based on collected properties during iteration.
pub trait BlockPropertyFilter: Send + Sync {
    /// Return true if this block should be SKIPPED (does not match the query).
    fn should_skip(&self, properties: &[u8]) -> bool;
    /// Name must match the corresponding BlockPropertyCollector's name.
    fn name(&self) -> &str;
}
