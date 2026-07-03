//! Configuration options for MMDB.

use std::{fmt, sync::Arc};

use crate::sst::format::CompressionType;

/// Callback that returns `true` for user keys that should be skipped during iteration.
pub type SkipPointFn = Arc<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// Database-level options.
#[derive(Clone)]
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
    /// Maximum number of TableReader entries retained by the table cache.
    /// Live Versions and iterators can pin additional readers.
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
    /// Optional compaction filter. Wrapped in `Arc` so it survives Clone
    /// and is shared with background compaction threads.
    pub compaction_filter: Option<Arc<dyn CompactionFilter>>,

    // ---- Compaction parallelism (RocksDB: increase_parallelism) ----
    /// Maximum number of background compaction threads. Default: 1.
    /// RocksDB equivalent: `max_background_compactions` / `increase_parallelism`.
    pub max_background_compactions: usize,
    /// Maximum sub-compactions per compaction job. Default: 1 (no sub-compaction).
    /// RocksDB equivalent: `max_subcompactions`.
    /// When > 1, a single compaction is split into parallel sub-tasks by key range
    /// using target-level file boundaries as split points. Effective only when the
    /// target level has enough files to split on.
    pub max_subcompactions: usize,

    // ---- Cache behavior ----
    /// Pin L0 index and filter blocks in block cache (never evict). Default: true.
    /// RocksDB equivalent: `pin_l0_filter_and_index_blocks_in_cache`.
    pub pin_l0_filter_and_index_blocks_in_cache: bool,

    /// Factories for block property collectors. Each factory is called once per SST
    /// file build to produce a fresh collector instance.
    pub block_property_collectors:
        Vec<Arc<dyn Fn() -> Box<dyn BlockPropertyCollector> + Send + Sync>>,
    /// When the number of keys registered via [`DB::lazy_delete_batch`]
    /// reaches this threshold, a background compaction is automatically
    /// signalled. Set to 0 to disable auto-triggering.
    pub lazy_delete_compaction_threshold: usize,
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
            block_property_collectors: Vec::new(),
            lazy_delete_compaction_threshold: 10_000,
        }
    }
}

impl fmt::Debug for DbOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbOptions")
            .field("create_if_missing", &self.create_if_missing)
            .field("error_if_exists", &self.error_if_exists)
            .field("write_buffer_size", &self.write_buffer_size)
            .field("max_immutable_memtables", &self.max_immutable_memtables)
            .field("l0_compaction_trigger", &self.l0_compaction_trigger)
            .field("target_file_size_base", &self.target_file_size_base)
            .field("max_bytes_for_level_base", &self.max_bytes_for_level_base)
            .field(
                "max_bytes_for_level_multiplier",
                &self.max_bytes_for_level_multiplier,
            )
            .field("num_levels", &self.num_levels)
            .field("block_size", &self.block_size)
            .field("block_restart_interval", &self.block_restart_interval)
            .field("bloom_bits_per_key", &self.bloom_bits_per_key)
            .field("compression", &self.compression)
            .field("block_cache_capacity", &self.block_cache_capacity)
            .field("max_open_files", &self.max_open_files)
            .field("l0_slowdown_trigger", &self.l0_slowdown_trigger)
            .field("l0_stop_trigger", &self.l0_stop_trigger)
            .field(
                "rate_limiter_bytes_per_sec",
                &self.rate_limiter_bytes_per_sec,
            )
            .field("prefix_len", &self.prefix_len)
            .field("compression_per_level", &self.compression_per_level)
            .field(
                "compaction_filter",
                &self.compaction_filter.as_ref().map(|_| ".."),
            )
            .field(
                "max_background_compactions",
                &self.max_background_compactions,
            )
            .field("max_subcompactions", &self.max_subcompactions)
            .field(
                "pin_l0_filter_and_index_blocks_in_cache",
                &self.pin_l0_filter_and_index_blocks_in_cache,
            )
            .field(
                "block_property_collectors",
                &self.block_property_collectors.len(),
            )
            .field(
                "lazy_delete_compaction_threshold",
                &self.lazy_delete_compaction_threshold,
            )
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
    /// If set, reads will use this snapshot sequence number. Values above the
    /// current committed sequence are clamped to it, so a snapshot can never
    /// observe uncommitted or partially-applied writes.
    ///
    /// Obtain a registered sequence via [`crate::DB::snapshot`] (see
    /// [`crate::Snapshot::read_options`]); a raw sequence number that is not
    /// registered as a snapshot may see its data garbage-collected by
    /// compaction.
    pub snapshot: Option<u64>,
    /// Whether a block-cache miss inserts the block into the cache. Default: true.
    ///
    /// Set to `false` for large scans (analytics, backups, bulk exports) so
    /// one-shot blocks don't evict hot point-read blocks. Cache *hits* are
    /// still served from the cache either way. RocksDB equivalent: `fill_cache`.
    pub fill_cache: bool,
    /// Optional callback checked during iteration. If it returns `true` for a
    /// user key, that key is skipped without being yielded.
    pub skip_point: Option<SkipPointFn>,
    /// Block property filters to skip entire data blocks during iteration.
    pub block_property_filters: Vec<Arc<dyn BlockPropertyFilter>>,
    /// Inclusive lower bound on user keys for iteration. Keys < this are skipped.
    /// Enforced by DBIterator — no manual bound checking needed.
    /// RocksDB equivalent: `iterate_lower_bound`.
    pub iterate_lower_bound: Option<Vec<u8>>,
    /// Exclusive upper bound on user keys for iteration. Keys >= this are skipped.
    /// Enforced by DBIterator — no manual bound checking needed.
    /// RocksDB equivalent: `iterate_upper_bound`.
    pub iterate_upper_bound: Option<Vec<u8>>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            snapshot: None,
            fill_cache: true,
            skip_point: None,
            block_property_filters: Vec::new(),
            iterate_lower_bound: None,
            iterate_upper_bound: None,
        }
    }
}

impl fmt::Debug for ReadOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadOptions")
            .field("snapshot", &self.snapshot)
            .field("fill_cache", &self.fill_cache)
            .field("skip_point", &self.skip_point.as_ref().map(|_| ".."))
            .field("block_property_filters", &self.block_property_filters.len())
            .field("iterate_lower_bound", &self.iterate_lower_bound)
            .field("iterate_upper_bound", &self.iterate_upper_bound)
            .finish()
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

    /// Returns `true` if this filter never removes or changes any entry, i.e.
    /// [`filter`](Self::filter) always returns [`CompactionFilterDecision::Keep`].
    ///
    /// When `true`, the engine may apply the trivial-move optimization
    /// (relocating a single SST to the next level without rewriting it) instead
    /// of streaming every entry through the filter. The default is `false`
    /// (conservative — the filter is assumed to be active).
    fn is_noop(&self) -> bool {
        false
    }
}

impl fmt::Debug for dyn CompactionFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
