//! Configuration options for MMDB.

use std::{
    borrow::Cow,
    fmt,
    sync::{Arc, LazyLock},
};

use crate::db::SnapshotRef;
use crate::sst::format::CompressionType;

/// Callback that returns `true` for user keys that should be skipped during iteration.
pub type SkipPointFn = Arc<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// Database-level options.
#[derive(Clone)]
#[non_exhaustive]
pub struct DbOptions {
    /// Create the database if it does not already exist.
    ///
    /// Existence is defined by the `CURRENT` marker (same as
    /// [`Self::error_if_exists`]), not by the path merely being a directory.
    /// When false, opening an empty directory fails without creating files.
    pub create_if_missing: bool,
    /// Return an error if the database already exists (`CURRENT` is present).
    pub error_if_exists: bool,
    /// Size of a single MemTable in bytes before it is frozen.
    pub write_buffer_size: usize,
    /// Maximum number of frozen memtables awaiting durable installation.
    pub max_immutable_memtables: usize,
    /// Byte interval for advisory WAL writeback. Zero disables the hint.
    pub wal_bytes_per_sync: u64,
    /// Number of L0 files that triggers compaction.
    pub l0_compaction_trigger: usize,
    /// Target size for SST files in bytes.
    pub target_file_size_base: u64,
    /// Max total size of L1 in bytes.
    pub max_bytes_for_level_base: u64,
    /// Multiplier between levels.
    pub max_bytes_for_level_multiplier: f64,
    /// Persisted number of levels, including L0. Valid values are 2 through 16.
    pub num_levels: usize,
    /// Block size for SST data blocks.
    pub block_size: usize,
    /// Restart interval for prefix compression in data blocks.
    pub block_restart_interval: usize,
    /// Bits per key for bloom filter. 0 disables bloom filter. A writable
    /// open rejects a value whose filter for a single key would exceed the
    /// maximum readable block size.
    pub bloom_bits_per_key: u32,
    /// Compression type for SST data blocks.
    pub compression: CompressionType,
    /// Block cache capacity in bytes. 0 disables caching.
    pub block_cache_capacity: u64,
    /// Maximum number of TableReader entries retained by the table cache.
    /// Live Versions and iterators can pin additional readers.
    pub max_open_files: u64,
    /// Number of L0 files that triggers write slowdown.
    /// Must be <= `l0_stop_trigger` (validated at [`crate::DB::open`]).
    pub l0_slowdown_trigger: usize,
    /// Number of L0 files that stops writes until compaction completes.
    /// Must be >= `l0_slowdown_trigger` (validated at [`crate::DB::open`]).
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
    /// Values below 1 are treated as 1: a writable handle always runs at
    /// least one compaction thread.
    /// RocksDB equivalent: `max_background_compactions` / `increase_parallelism`.
    pub max_background_compactions: usize,
    /// Maximum sub-compactions per compaction job. Default: 1 (no sub-compaction).
    /// RocksDB equivalent: `max_subcompactions`.
    /// When > 1, a single compaction is split into parallel sub-tasks by key range
    /// using target-level file boundaries as split points. The actual count is
    /// bounded by those boundaries; L0 compactions run serially.
    pub max_subcompactions: usize,

    // ---- Cache behavior ----
    /// Warm the index and pin the first data block of newly installed L0 files.
    pub pin_l0_first_block_and_warm_index: bool,
    /// Omit point-lookup Bloom filters (`filter.bloom`) on SSTs written to the
    /// bottommost level (`num_levels - 1`), saving ~85–90% of filter memory on
    /// workloads where point queries predominantly hit existing keys. Prefix
    /// Bloom filters (`filter.prefix`, when `prefix_len > 0`) are still built.
    /// Default: `false`.
    pub optimize_filters_for_hits: bool,

    /// Factories for block property collectors. Each factory is called once per SST
    /// file build to produce a fresh collector instance.
    pub block_property_collectors:
        Vec<Arc<dyn Fn() -> Box<dyn BlockPropertyCollector> + Send + Sync>>,
    /// When the number of keys registered via [`DB::lazy_delete`](crate::DB::lazy_delete)
    /// or [`DB::lazy_delete_batch`](crate::DB::lazy_delete_batch)
    /// reaches this threshold, a background sweep is automatically
    /// scheduled: every populated level is force-rewritten through the
    /// compaction filter so the registered keys are physically removed
    /// even when no organic compaction is pending. The worker builds SSTs
    /// outside the DB mutex, then validates its inputs and active snapshots
    /// before installation. Default: 0 (disabled).
    pub lazy_delete_compaction_threshold: usize,
    /// Optional shared block-cache pool. `None` (the default) gives this
    /// DB a private cache sized by `block_cache_capacity` — the
    /// historical behavior, unchanged. `Some(pool)` attaches this DB to
    /// the caller-owned pool: DBs given the same `Arc` share one LRU
    /// capacity (every key is namespaced by a per-DB member id, so
    /// sharing is a capacity decision, never a correctness one), and
    /// **`block_cache_capacity` is ignored** — capacity belongs to the
    /// pool.
    pub block_cache: Option<Arc<crate::cache::block_cache::BlockCachePool>>,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            error_if_exists: false,
            write_buffer_size: 64 * 1024 * 1024, // 64 MB
            max_immutable_memtables: 4,
            wal_bytes_per_sync: 2 << 20,
            l0_compaction_trigger: 4,
            target_file_size_base: 64 * 1024 * 1024, // 64 MB
            max_bytes_for_level_base: 256 * 1024 * 1024, // 256 MB
            max_bytes_for_level_multiplier: 10.0,
            num_levels: 7,
            block_size: 4096,
            block_restart_interval: 16,
            bloom_bits_per_key: 12,
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
            pin_l0_first_block_and_warm_index: true,
            optimize_filters_for_hits: false,
            block_property_collectors: Vec::new(),
            lazy_delete_compaction_threshold: 0,
            block_cache: None,
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
            .field("wal_bytes_per_sync", &self.wal_bytes_per_sync)
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
                "pin_l0_first_block_and_warm_index",
                &self.pin_l0_first_block_and_warm_index,
            )
            .field("optimize_filters_for_hits", &self.optimize_filters_for_hits)
            .field(
                "block_property_collectors",
                &self.block_property_collectors.len(),
            )
            .field(
                "lazy_delete_compaction_threshold",
                &self.lazy_delete_compaction_threshold,
            )
            .field("block_cache", &self.block_cache.as_ref().map(|_| ".."))
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
            pin_l0_first_block_and_warm_index: true,
            ..Default::default()
        }
    }
}

/// Options for read operations.
#[derive(Clone)]
#[non_exhaustive]
pub struct ReadOptions<'a> {
    /// Registered snapshot from the same open database.
    pub snapshot: Option<SnapshotRef<'a>>,
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
    pub iterate_lower_bound: Option<Cow<'a, [u8]>>,
    /// Exclusive upper bound on user keys for iteration. Keys >= this are skipped.
    /// Enforced by DBIterator — no manual bound checking needed.
    /// RocksDB equivalent: `iterate_upper_bound`.
    pub iterate_upper_bound: Option<Cow<'a, [u8]>>,
}

impl Default for ReadOptions<'_> {
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

impl fmt::Debug for ReadOptions<'_> {
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

/// Durability of an acknowledged batch.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum DurabilityMode {
    /// Append to the WAL and flush userspace buffers before acknowledgement.
    #[default]
    WalAsync,
    /// Confirm WAL data durability before acknowledgement.
    WalSync,
    /// Apply only to memory. A crash before a durable flush may lose this batch.
    DisableWal,
}

/// Options for write operations.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct WriteOptions {
    /// WAL policy for this batch.
    pub durability: DurabilityMode,
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
/// [`filter`](Self::filter) is not invoked for every key-value pair a
/// compaction touches — only when all of the following hold:
///
/// - The compaction's target level is the bottommost level for the affected
///   key range. Applying `Remove` or `ChangeValue` above the bottommost level
///   could otherwise resurrect stale data, since an older, unfiltered version
///   of the same key may still live in a lower level and would then
///   incorrectly become visible.
/// - No explicit snapshots are currently active (handles from
///   [`crate::DB::snapshot`]). Removing or changing a value while such a
///   snapshot is open could change what that snapshot observes, violating its
///   MVCC visibility guarantee. Filtering therefore pauses while any
///   registered snapshot remains open and resumes once the last one is
///   released. Ordinary DB iterators pin a SuperVersion for their own
///   read sequence and are **not** registered in the snapshot list; they
///   do not by themselves pause filtering.
/// - The entry is a `Value`. `Deletion` and `RangeDeletion` entries are never
///   passed to the filter.
///
/// Callbacks may read their database. A callback that calls a write, WAL fence,
/// flush, explicit compaction, or close on the same database receives
/// [`ErrorKind::InvalidArgument`](crate::ErrorKind::InvalidArgument).
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
///
/// Each name and serialized property must fit in 65,535 bytes, with at most
/// 65,535 properties per block. Properties from all data blocks also share
/// the SST's index budget with keys and framing: the builder reserves space
/// below the reader's 64 MiB decompressed block limit. Excessive generated
/// metadata makes the flush or compaction return `InvalidArgument`.
///
/// Callbacks may read their database. A callback that calls a write, WAL fence,
/// flush, explicit compaction, or close on the same database receives
/// [`ErrorKind::InvalidArgument`](crate::ErrorKind::InvalidArgument).
pub trait BlockPropertyCollector: Send + Sync {
    /// Called with the user key of every point value or point deletion in the block.
    fn add(&mut self, user_key: &[u8]);
    /// Called when a data block is flushed. Returns serialized properties
    /// for this block. The collector is then reset for the next block.
    fn finish_block(&mut self) -> Vec<u8>;
    /// Stable name identifying the property encoding and semantics for the store lifetime.
    fn name(&self) -> &str;
}

/// Filters data blocks based on collected properties during iteration.
pub trait BlockPropertyFilter: Send + Sync {
    /// Test the visible user key after MVCC resolution. Point reads ignore this filter.
    fn key_matches(&self, user_key: &[u8]) -> bool;

    /// May return true only if every key collected in the block fails `key_matches`.
    fn should_skip(&self, properties: &[u8]) -> bool;
    /// Name must match the corresponding BlockPropertyCollector's name.
    fn name(&self) -> &str;
}

/// Owned once per scan, shared by all physical sources.
#[derive(Clone, Default)]
pub(crate) struct IterConfig {
    pub prefix: Option<Arc<[u8]>>,
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
    pub filters: Arc<[Arc<dyn BlockPropertyFilter>]>,
}

impl IterConfig {
    pub fn new(
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        filters: &[Arc<dyn BlockPropertyFilter>],
    ) -> Self {
        Self {
            prefix: None,
            lower: lower.map(<[u8]>::to_vec),
            upper: upper.map(<[u8]>::to_vec),
            filters: filters.into(),
        }
    }

    pub fn empty() -> Arc<Self> {
        static EMPTY: LazyLock<Arc<IterConfig>> = LazyLock::new(Arc::default);
        EMPTY.clone()
    }

    pub fn matches(&self, key: &[u8]) -> bool {
        self.filters.iter().all(|f| f.key_matches(key))
    }
}

macro_rules! option_builder {
    ($method:ident, $field:ident, $ty:ty) => {
        #[doc = concat!("Set [`", stringify!($field), "`](Self::", stringify!($field), ").")]
        #[must_use]
        pub fn $method(mut self, value: $ty) -> Self {
            self.$field = value;
            self
        }
    };
}

impl DbOptions {
    option_builder!(with_create_if_missing, create_if_missing, bool);
    option_builder!(with_error_if_exists, error_if_exists, bool);
    option_builder!(with_write_buffer_size, write_buffer_size, usize);
    option_builder!(with_max_immutable_memtables, max_immutable_memtables, usize);
    option_builder!(with_wal_bytes_per_sync, wal_bytes_per_sync, u64);
    option_builder!(with_l0_compaction_trigger, l0_compaction_trigger, usize);
    option_builder!(with_target_file_size_base, target_file_size_base, u64);
    option_builder!(with_max_bytes_for_level_base, max_bytes_for_level_base, u64);
    option_builder!(
        with_max_bytes_for_level_multiplier,
        max_bytes_for_level_multiplier,
        f64
    );
    option_builder!(with_num_levels, num_levels, usize);
    option_builder!(with_block_size, block_size, usize);
    option_builder!(with_block_restart_interval, block_restart_interval, usize);
    option_builder!(with_bloom_bits_per_key, bloom_bits_per_key, u32);
    option_builder!(with_compression, compression, CompressionType);
    option_builder!(with_block_cache_capacity, block_cache_capacity, u64);
    option_builder!(with_max_open_files, max_open_files, u64);
    option_builder!(with_l0_slowdown_trigger, l0_slowdown_trigger, usize);
    option_builder!(with_l0_stop_trigger, l0_stop_trigger, usize);
    option_builder!(
        with_rate_limiter_bytes_per_sec,
        rate_limiter_bytes_per_sec,
        u64
    );
    option_builder!(with_prefix_len, prefix_len, usize);
    option_builder!(
        with_compression_per_level,
        compression_per_level,
        Vec<CompressionType>
    );
    option_builder!(
        with_compaction_filter,
        compaction_filter,
        Option<Arc<dyn CompactionFilter>>
    );
    option_builder!(
        with_max_background_compactions,
        max_background_compactions,
        usize
    );
    option_builder!(with_max_subcompactions, max_subcompactions, usize);
    option_builder!(
        with_pin_l0_first_block_and_warm_index,
        pin_l0_first_block_and_warm_index,
        bool
    );
    option_builder!(
        with_optimize_filters_for_hits,
        optimize_filters_for_hits,
        bool
    );
    option_builder!(
        with_block_property_collectors,
        block_property_collectors,
        Vec<Arc<dyn Fn() -> Box<dyn BlockPropertyCollector> + Send + Sync>>
    );
    option_builder!(
        with_lazy_delete_compaction_threshold,
        lazy_delete_compaction_threshold,
        usize
    );
    option_builder!(
        with_block_cache,
        block_cache,
        Option<Arc<crate::cache::block_cache::BlockCachePool>>
    );

    /// Compute the Monkey level-proportional point-filter `bits_per_key` for `level`.
    ///
    /// Allocates `(ln R / ln^2 2) * (L_bottom - level)` extra bits per key to
    /// exponentially smaller shallower levels (clamped to `24` for standard `<= 24`
    /// configurations) and `max(min(b, 8), b - 1)` at `L_bottom` (or `0` when
    /// [`Self::optimize_filters_for_hits`] is enabled), cutting multi-level point-miss
    /// false positives by ~6x without increasing total filter RAM.
    pub(crate) fn bloom_bits_for_level(&self, level: usize) -> u32 {
        let base = self.bloom_bits_per_key;
        if base == 0 {
            return 0;
        }
        let bottom = self.num_levels.saturating_sub(1);
        if level >= bottom {
            if self.optimize_filters_for_hits {
                return 0;
            }
            if base > 24 || bottom == 0 {
                return base;
            }
            return base.saturating_sub(1).max(base.min(8));
        }
        if base > 24
            || !self.max_bytes_for_level_multiplier.is_finite()
            || self.max_bytes_for_level_multiplier <= 1.0
        {
            return base;
        }
        let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let delta = (self.max_bytes_for_level_multiplier.ln() / ln2_sq) * (bottom - level) as f64;
        let target = (base as f64 + delta).ceil();
        if target >= 24.0 {
            24
        } else {
            (target as u32).max(base)
        }
    }
}

impl<'a> ReadOptions<'a> {
    /// Read at a registered snapshot.
    #[must_use]
    pub fn with_snapshot(mut self, snapshot: impl Into<SnapshotRef<'a>>) -> Self {
        self.snapshot = Some(snapshot.into());
        self
    }
    option_builder!(with_fill_cache, fill_cache, bool);
    option_builder!(with_skip_point, skip_point, Option<SkipPointFn>);
    option_builder!(
        with_block_property_filters,
        block_property_filters,
        Vec<Arc<dyn BlockPropertyFilter>>
    );
    /// Set [`iterate_lower_bound`](Self::iterate_lower_bound).
    #[must_use]
    pub fn with_iterate_lower_bound(mut self, value: impl Into<Cow<'a, [u8]>>) -> Self {
        self.iterate_lower_bound = Some(value.into());
        self
    }
    /// Set [`iterate_upper_bound`](Self::iterate_upper_bound).
    #[must_use]
    pub fn with_iterate_upper_bound(mut self, value: impl Into<Cow<'a, [u8]>>) -> Self {
        self.iterate_upper_bound = Some(value.into());
        self
    }
}

impl WriteOptions {
    option_builder!(with_durability, durability, DurabilityMode);
    option_builder!(with_no_slowdown, no_slowdown, bool);
}
