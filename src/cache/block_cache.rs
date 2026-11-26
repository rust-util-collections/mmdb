//! SST block caches backed by moka. A [`BlockCachePool`] shares weighted
//! storage across DBs. Each [`BlockCache`] member has a distinct cache-key
//! namespace and its own L0 block pins. [`BlockCache::new`] creates a
//! private pool with a single member.

use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher, Hasher},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use arc_swap::ArcSwap;
use moka::notification::RemovalCause;
use parking_lot::Mutex;

use crate::sst::block::Block;

/// Pool cache key: (member_id, sst_file_number, block_offset).
type CacheKey = (u64, u64, u64);

/// Fast non-cryptographic 64-bit mixer for internal integer cache keys.
/// Keys are engine-assigned `(member_id, file_number, block_offset)` tuples,
/// never caller-controlled strings. SplitMix64 finalization avalanches all 64
/// bits so both moka's high-bit segment selector and low-bit bucket masks see
/// uniform bit dispersion.
#[derive(Clone, Copy, Default)]
pub(crate) struct FastBuildHasher;

impl BuildHasher for FastBuildHasher {
    type Hasher = FastHasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        FastHasher(0x517c_c1b7_2722_0a95)
    }
}

pub(crate) struct FastHasher(u64);

impl Hasher for FastHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        }
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = (self.0.rotate_left(5) ^ i).wrapping_mul(0x517c_c1b7_2722_0a95);
    }

    #[inline]
    fn finish(&self) -> u64 {
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// Role of a pre-validated `Block` stored in [`BlockCache`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValidatedKind {
    Data,
    DataOrdered,
    Index,
    RangeDel,
}

/// Shared cached block entry holding both the backing bytes and optional
/// pre-validated `Block` view behind a single `Arc` reference count.
pub(crate) struct CachedBlock {
    /// Holds the raw bytes (`restart_offset == usize::MAX` before `Block` validation).
    block: Block,
    /// On-disk payload size from `BlockHandle::size`, or `u64::MAX` for raw inserts.
    compressed_size: u64,
    kind: Option<ValidatedKind>,
    /// Upgrades `ValidatedKind::Data` to `DataOrdered` in place with zero allocations.
    data_ordered: AtomicBool,
}

impl CachedBlock {
    #[inline]
    pub(crate) fn compressed_size(&self) -> u64 {
        self.compressed_size
    }

    #[inline]
    pub(crate) fn raw_arc(&self) -> &Arc<Vec<u8>> {
        self.block.data_arc()
    }

    /// Borrow the pre-validated `Block` and its effective role without cloning
    /// the inner `Arc<Vec<u8>>`.
    #[inline]
    pub(crate) fn validated_ref(&self) -> Option<(&Block, ValidatedKind)> {
        let mut kind = self.kind?;
        if self.block.data_end_offset() == usize::MAX {
            return None;
        }
        if kind == ValidatedKind::Data && self.data_ordered.load(Ordering::Acquire) {
            kind = ValidatedKind::DataOrdered;
        }
        Some((&self.block, kind))
    }

    #[inline]
    pub(crate) fn mark_data_ordered(&self) {
        self.data_ordered.store(true, Ordering::Release);
    }
}

/// Cached block handle cloned on moka / pin-map lookups with a single atomic RMW.
#[derive(Clone)]
struct CacheValue(Arc<CachedBlock>);

impl CacheValue {
    fn raw(data: Arc<Vec<u8>>) -> Self {
        Self(Arc::new(CachedBlock {
            block: Block::from_validated(data, usize::MAX, 0),
            compressed_size: u64::MAX,
            kind: None,
            data_ordered: AtomicBool::new(false),
        }))
    }

    fn from_block(block: &Block, compressed_size: u64, kind: ValidatedKind) -> Self {
        let ordered = kind == ValidatedKind::DataOrdered;
        Self(Arc::new(CachedBlock {
            block: block.clone(),
            compressed_size,
            kind: Some(kind),
            data_ordered: AtomicBool::new(ordered),
        }))
    }
}

/// Immutable member-local pins, published together with their byte total.
#[derive(Clone, Default)]
struct PinnedBlocks {
    entries: HashMap<(u64, u64), CacheValue, FastBuildHasher>,
    bytes: u64,
}

/// Number of shards in the reverse index. Must be a power of two.
///
/// With a single-mutex index, every insert (the read-miss path) and
/// every eviction-listener callback of every member DB would serialize
/// on one lock; sharding bounds that fan-in.
const FO_SHARDS: usize = 16;

#[cfg(test)]
thread_local! {
    static MAINTAIN_AFTER_INSERT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static BEFORE_TAKE_FILE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Maximum number of internal segments in the pool's LRU store.
///
/// A pool concentrates the hit traffic of every member DB (e.g. 16
/// engine shards x N reader threads) onto one cache instance; a plain
/// `moka::sync::Cache` serializes enough of that on its internal
/// read-op bookkeeping to regress all-hit concurrent reads measurably
/// (vsdb Q1 bench, 8 threads x 16 members, 100%-hit uniform reads:
/// +14.6% vs private caches unsegmented; +6.8% at 16 segments; noise
/// at 64 segments, while the skewed-load win stays at -72%/-81%).
/// `SegmentedCache` partitions by key hash, restoring the parallelism
/// the private-per-DB layout had. Small caches use fewer segments so a
/// normal block that fits the whole cache also fits one segment.
const MAX_LRU_SEGMENTS: usize = 64;
/// Preserve at least 1 MiB of admission capacity per segment. This keeps
/// the benchmarked 64-segment layout unchanged for the default 64 MiB cache.
const MIN_LRU_SEGMENT_CAPACITY: u64 = 1024 * 1024;
const _: () = assert!(FO_SHARDS.is_power_of_two());

fn lru_segments(capacity_bytes: u64) -> usize {
    let raw =
        (capacity_bytes / MIN_LRU_SEGMENT_CAPACITY).clamp(1, MAX_LRU_SEGMENTS as u64) as usize;
    let rounded_up = raw.next_power_of_two();
    if rounded_up == raw {
        raw
    } else {
        rounded_up / 2
    }
}

/// One shard of the reverse index: `(member, file_number)` → offsets.
type FoShard = Mutex<HashMap<(u64, u64), HashSet<u64, FastBuildHasher>, FastBuildHasher>>;

/// Reverse index: which block offsets belong to each (member, file),
/// used for bulk invalidation. Shared with the moka eviction listener
/// so LRU-evicted offsets are pruned automatically (otherwise it would
/// grow without bound).
struct FileOffsetsIndex {
    shards: [FoShard; FO_SHARDS],
}

impl FileOffsetsIndex {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::with_hasher(FastBuildHasher))),
        }
    }

    #[inline]
    fn shard_for(member: u64, file_number: u64) -> usize {
        // Cheap avalanche mix; both halves of the key must influence the
        // shard so one member's files (and one hot file) spread out.
        let h = (member ^ file_number.rotate_left(17)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        (h >> 60) as usize & (FO_SHARDS - 1)
    }

    fn add(&self, member: u64, file_number: u64, block_offset: u64) {
        self.shards[Self::shard_for(member, file_number)]
            .lock()
            .entry((member, file_number))
            .or_default()
            .insert(block_offset);
    }

    /// Prune the offset when its cache entry leaves the store. Moka also
    /// notifies on replacement, which leaves the new value at the same key.
    fn on_removal(&self, &(member, file_number, block_offset): &CacheKey, cause: RemovalCause) {
        if cause == RemovalCause::Replaced {
            return;
        }
        let mut map = self.shards[Self::shard_for(member, file_number)].lock();
        if let Some(set) = map.get_mut(&(member, file_number)) {
            set.remove(&block_offset);
            if set.is_empty() {
                map.remove(&(member, file_number));
            }
        }
    }

    /// Remove and return every tracked offset of one (member, file).
    fn take_file(&self, member: u64, file_number: u64) -> Option<HashSet<u64, FastBuildHasher>> {
        #[cfg(test)]
        if let Some(hook) = BEFORE_TAKE_FILE.with(|cell| cell.borrow_mut().take()) {
            hook();
        }
        self.shards[Self::shard_for(member, file_number)]
            .lock()
            .remove(&(member, file_number))
    }

    /// Remove and return every tracked (file, offsets) of one member —
    /// the detach sweep. O(total index entries), acceptable: detach is
    /// a close-time event.
    fn take_member(&self, member: u64) -> Vec<(u64, HashSet<u64, FastBuildHasher>)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            let mut map = shard.lock();
            let files: Vec<u64> = map
                .keys()
                .filter(|&&(m, _)| m == member)
                .map(|&(_, f)| f)
                .collect();
            for f in files {
                if let Some(set) = map.remove(&(member, f)) {
                    out.push((f, set));
                }
            }
        }
        out
    }
}

/// Capacity and approximate evictable charge of a whole [`BlockCachePool`].
///
/// Each store charges the capacity of an admitted block's byte buffer, with a
/// minimum charge of one byte. Cache maintenance updates the charged totals
/// asynchronously. Member-local L0 pins are accounted for by
/// [`DB::memory_usage`](crate::DB::memory_usage).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BlockCachePoolUsage {
    /// Configured capacity in bytes across all segments of the main store.
    pub segmented_capacity_bytes: u64,
    /// Configured capacity in bytes of the separate overflow store.
    pub overflow_capacity_bytes: u64,
    /// Approximate charge in bytes across all segments of the main store.
    pub segmented_weighted_bytes: u64,
    /// Approximate charge in bytes in the overflow store.
    pub overflow_weighted_bytes: u64,
}

/// Shared weighted storage for one or more DBs' block caches.
///
/// Blocks that fit a segment's share use the segmented cache. Blocks larger
/// than that share and within the configured capacity use an unsegmented
/// overflow cache. Each store has its own capacity limit and weighs a block
/// by its buffer capacity. Their combined weight can exceed the configured
/// capacity. L0 pins are accounted for separately on each member.
///
/// Supply the same `Arc<BlockCachePool>` to multiple DBs through
/// [`DbOptions::block_cache`](crate::DbOptions::block_cache) to share these stores.
pub struct BlockCachePool {
    inner: moka::sync::SegmentedCache<CacheKey, CacheValue, FastBuildHasher>,
    /// Unsegmented fallback for valid blocks whose weight exceeds a single
    /// segment's share and fit within `capacity_bytes`.
    overflow: moka::sync::Cache<CacheKey, CacheValue, FastBuildHasher>,
    has_overflow: AtomicBool,
    capacity_bytes: u64,
    segment_capacity: u64,
    /// Track which offsets belong to each (member, file) for bulk invalidation.
    index: Arc<FileOffsetsIndex>,
    /// Member-id allocator for [`attach`](Self::attach).
    next_member: AtomicU64,
    /// When true (capacity 0), caching is disabled: inserts are no-ops and
    /// lookups always miss. Honors the documented "0 disables caching" option.
    disabled: bool,
}

impl BlockCachePool {
    /// Set the capacity in bytes for each of the segmented and overflow stores.
    /// A capacity of 0 disables caching entirely.
    pub fn new(capacity_bytes: u64) -> Self {
        let index = Arc::new(FileOffsetsIndex::new());
        let segments = lru_segments(capacity_bytes);
        let segment_capacity = capacity_bytes / segments as u64;
        let listener_index = index.clone();
        let inner = moka::sync::SegmentedCache::builder(segments)
            .max_capacity(capacity_bytes)
            .weigher(|_key: &CacheKey, value: &CacheValue| -> u32 {
                value.0.raw_arc().capacity().max(1) as u32
            })
            .eviction_listener(
                move |key: Arc<CacheKey>, _value: CacheValue, cause: RemovalCause| {
                    listener_index.on_removal(&key, cause);
                },
            )
            .build_with_hasher(FastBuildHasher);
        let overflow_index = index.clone();
        let overflow = moka::sync::Cache::builder()
            .max_capacity(capacity_bytes)
            .weigher(|_key: &CacheKey, value: &CacheValue| -> u32 {
                value.0.raw_arc().capacity().max(1) as u32
            })
            .eviction_listener(
                move |key: Arc<CacheKey>, _value: CacheValue, cause: RemovalCause| {
                    overflow_index.on_removal(&key, cause);
                },
            )
            .build_with_hasher(FastBuildHasher);
        Self {
            inner,
            overflow,
            has_overflow: AtomicBool::new(false),
            capacity_bytes,
            segment_capacity,
            index,
            next_member: AtomicU64::new(0),
            disabled: capacity_bytes == 0,
        }
    }

    /// Join the pool: returns a new member's view. Member ids are
    /// pool-unique and never reused, so a detached member's stale keys
    /// can never alias a later member's.
    pub fn attach(self: &Arc<Self>) -> BlockCache {
        BlockCache {
            pool: self.clone(),
            member: self.next_member.fetch_add(1, Ordering::Relaxed),
            detached: AtomicBool::new(false),
            has_pins: AtomicBool::new(false),
            pinned: ArcSwap::from_pointee(PinnedBlocks::default()),
            pin_updates: Mutex::new(()),
        }
    }

    /// Approximate entry count across both stores and all members, excluding pins.
    pub fn entry_count(&self) -> u64 {
        let extra = if self.has_overflow.load(Ordering::Acquire) {
            self.overflow.entry_count()
        } else {
            0
        };
        self.inner.entry_count() + extra
    }

    /// Sample both evictable stores across every member. Count each pool once
    /// when accounting for a group of DB handles sharing it.
    pub fn usage(&self) -> BlockCachePoolUsage {
        BlockCachePoolUsage {
            segmented_capacity_bytes: self.capacity_bytes,
            overflow_capacity_bytes: self.capacity_bytes,
            segmented_weighted_bytes: self.inner.weighted_size(),
            overflow_weighted_bytes: self.overflow.weighted_size(),
        }
    }

    /// Test-only: drain moka's pending maintenance so [`entry_count`](Self::entry_count)
    /// reflects inserts made earlier in the same thread.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn run_pending_tasks(&self) {
        self.inner.run_pending_tasks();
        if self.has_overflow.load(Ordering::Acquire) {
            self.overflow.run_pending_tasks();
        }
    }
}

/// One DB's view of a [`BlockCachePool`]. Cache keys include the member ID.
/// The member also owns its pinned entries, such as the first data block
/// of a newly flushed L0 file. See [`insert_pinned`](Self::insert_pinned).
pub struct BlockCache {
    pub(crate) pool: Arc<BlockCachePool>,
    member: u64,
    /// Set once by [`detach`](Self::detach); afterwards the view is a
    /// cache-bypass (misses on read, caches nothing on write). This is
    /// a cutoff, not a barrier: an in-flight *unpinned* insert racing
    /// the detach sweep may leave a transient orphan entry, which LRU
    /// reclaims like any cold block (its member id is retired, so it
    /// can never be read again). Pinned inserts cannot race the sweep:
    /// both sides serialize on the `pin_updates` mutex.
    detached: AtomicBool,
    /// True while `pinned` contains at least one entry, letting empty-pin
    /// members skip the `ArcSwap` guard and `HashMap` probe on every lookup.
    has_pins: AtomicBool,
    /// Pinned entries — never evicted by LRU. **Member-local** (keyed
    /// `(file_number, block_offset)`): pins are never shared, never
    /// weighed against the pool's capacity, and keeping them here means
    /// one member's pins add zero lock traffic to other members' `get`
    /// fast paths — exactly the per-DB behavior of the pre-pool cache.
    /// Readers retain an immutable snapshot without taking the update lock.
    /// Removed blocks stay valid for in-flight readers through their Arcs.
    pinned: ArcSwap<PinnedBlocks>,
    /// Serializes snapshot updates, including detach, so concurrent writers
    /// cannot overwrite each other's pins or restore a retired snapshot.
    pin_updates: Mutex<()>,
}

impl BlockCache {
    #[inline]
    pub(crate) fn is_enabled(&self) -> bool {
        !self.pool.disabled && !self.detached.load(Ordering::Relaxed)
    }

    /// Create a private pool with [`BlockCachePool::new`] and attach one member.
    /// A capacity of 0 disables caching entirely.
    pub fn new(capacity_bytes: u64) -> Self {
        Arc::new(BlockCachePool::new(capacity_bytes)).attach()
    }

    #[inline]
    pub(crate) fn get_cached_entry(
        &self,
        file_number: u64,
        block_offset: u64,
    ) -> Option<Arc<CachedBlock>> {
        if !self.is_enabled() {
            return None;
        }
        // The snapshot load never acquires the pin-update mutex, and is
        // skipped completely when this member holds no pins.
        if self.has_pins.load(Ordering::Acquire)
            && let Some(v) = self.pinned.load().entries.get(&(file_number, block_offset))
        {
            return Some(v.0.clone());
        }
        let key = (self.member, file_number, block_offset);
        if let Some(v) = self.pool.inner.get(&key) {
            return Some(v.0);
        }
        if self.pool.has_overflow.load(Ordering::Acquire) {
            return self.pool.overflow.get(&key).map(|v| v.0);
        }
        None
    }

    /// Look up a cached block. Pinned entries are checked first.
    pub fn get(&self, file_number: u64, block_offset: u64) -> Option<Arc<Vec<u8>>> {
        self.get_cached_entry(file_number, block_offset)
            .map(|entry| entry.raw_arc().clone())
    }

    fn insert_entry(&self, file_number: u64, block_offset: u64, value: CacheValue) {
        if !self.is_enabled() {
            return;
        }
        let key = (self.member, file_number, block_offset);
        // Moka weights are u32. A larger allocation cannot be charged in
        // full, and a replacement must retire any older cached value.
        let Ok(weight) = u32::try_from(value.0.raw_arc().capacity().max(1)) else {
            self.pool.inner.invalidate(&key);
            if self.pool.has_overflow.load(Ordering::Acquire) {
                self.pool.overflow.invalidate(&key);
            }
            return;
        };
        let weight_u64 = weight as u64;
        if weight_u64 > self.pool.segment_capacity && weight_u64 <= self.pool.capacity_bytes {
            self.pool.has_overflow.store(true, Ordering::Release);
            self.pool.inner.invalidate(&key);
            self.pool.index.add(self.member, file_number, block_offset);
            self.pool.overflow.insert(key, value);
            #[cfg(test)]
            if MAINTAIN_AFTER_INSERT.replace(false) {
                self.pool.overflow.run_pending_tasks();
            }
            return;
        }
        if self.pool.has_overflow.load(Ordering::Acquire) {
            self.pool.overflow.invalidate(&key);
        }
        self.pool.index.add(self.member, file_number, block_offset);
        self.pool.inner.insert(key, value);
        #[cfg(test)]
        if MAINTAIN_AFTER_INSERT.replace(false) {
            self.pool.inner.run_pending_tasks();
        }
    }

    /// Insert a block into the cache.
    ///
    /// Register the offset before moka can evict it, so eviction cannot
    /// leave bookkeeping behind after the cached block is gone.
    ///
    /// Registration and insertion are not atomic: `invalidate_file()` can
    /// clear the index and invalidate the key before insertion completes.
    /// A concurrent eviction can also race a fresh insertion at the same
    /// offset. The remaining cache bytes are bounded by LRU capacity
    /// and cannot alias another file because SST file numbers are never
    /// reused. As with detach, invalidation is a cutoff rather than a
    /// barrier against concurrent insertion.
    pub fn insert(&self, file_number: u64, block_offset: u64, data: Vec<u8>) -> Arc<Vec<u8>> {
        let arc = Arc::new(data);
        self.insert_entry(file_number, block_offset, CacheValue::raw(arc.clone()));
        arc
    }

    pub(crate) fn insert_validated_block(
        &self,
        file_number: u64,
        block_offset: u64,
        compressed_size: u64,
        block: &Block,
        kind: ValidatedKind,
    ) {
        self.insert_entry(
            file_number,
            block_offset,
            CacheValue::from_block(block, compressed_size, kind),
        );
    }

    fn insert_pinned_entry(&self, file_number: u64, block_offset: u64, value: CacheValue) {
        if self.pool.disabled {
            return;
        }
        let _update = self.pin_updates.lock();
        // Re-checked under the mutex: `detach` sweeps this map under the
        // same lock, so a pin can never slip in after the sweep (an
        // unremovable leak); see the `detached` field docs.
        if self.detached.load(Ordering::Relaxed) {
            return;
        }
        let mut pinned = (**self.pinned.load()).clone();
        let len = value.0.raw_arc().len() as u64;
        if let Some(old) = pinned.entries.insert((file_number, block_offset), value) {
            pinned.bytes -= old.0.raw_arc().len() as u64;
        }
        pinned.bytes += len;
        self.pinned.store(Arc::new(pinned));
        self.has_pins.store(true, Ordering::Release);
    }

    /// Insert a pinned block — never evicted by LRU.
    /// Used to pin the first data block (smallest key) of an L0 file, so the
    /// merging iterator's initial `peek()` is always a cache hit. Index leaves use ordinary cache admission.
    /// Filter lines and partition directories are retained by the table reader.
    pub fn insert_pinned(
        &self,
        file_number: u64,
        block_offset: u64,
        data: Vec<u8>,
    ) -> Arc<Vec<u8>> {
        let arc = Arc::new(data);
        self.insert_pinned_entry(file_number, block_offset, CacheValue::raw(arc.clone()));
        arc
    }

    pub(crate) fn insert_pinned_validated_block(
        &self,
        file_number: u64,
        block_offset: u64,
        compressed_size: u64,
        block: &Block,
        kind: ValidatedKind,
    ) {
        self.insert_pinned_entry(
            file_number,
            block_offset,
            CacheValue::from_block(block, compressed_size, kind),
        );
    }

    /// Unpin all entries for a specific file (e.g., when L0 file is compacted away).
    pub fn unpin_file(&self, file_number: u64) {
        if self.detached.load(Ordering::Relaxed) {
            return;
        }
        let _update = self.pin_updates.lock();
        let current = self.pinned.load();
        if !current.entries.keys().any(|&(f, _)| f == file_number) {
            return;
        }
        let mut pinned = (**current).clone();
        let mut removed_bytes = 0u64;
        pinned.entries.retain(|&(f, _), v| {
            if f == file_number {
                removed_bytes += v.0.raw_arc().len() as u64;
                false
            } else {
                true
            }
        });
        pinned.bytes -= removed_bytes;
        let has_any = !pinned.entries.is_empty();
        self.pinned.store(Arc::new(pinned));
        self.has_pins.store(has_any, Ordering::Release);
    }

    /// Invalidate all cached blocks this member holds for a specific file.
    /// Called after compaction removes an SST file. Other members'
    /// same-numbered files (unrelated DBs) are untouched.
    ///
    /// Reads-and-clears the reverse index (`take_file`) and invalidates
    /// only the offsets that snapshot contains — see `insert()`'s doc for
    /// races where concurrent insertion can outlive this invalidation.
    /// Accepted as a bounded, benign tradeoff rather than fixed.
    pub fn invalidate_file(&self, file_number: u64) {
        if self.detached.load(Ordering::Relaxed) {
            return;
        }
        self.unpin_file(file_number);
        if let Some(offsets) = self.pool.index.take_file(self.member, file_number) {
            let has_overflow = self.pool.has_overflow.load(Ordering::Acquire);
            for offset in offsets {
                let key = (self.member, file_number, offset);
                self.pool.inner.invalidate(&key);
                if has_overflow {
                    self.pool.overflow.invalidate(&key);
                }
            }
            self.pool.inner.run_pending_tasks();
            if has_overflow {
                self.pool.overflow.run_pending_tasks();
            }
        }
    }

    /// Leave the pool: sweep every entry this member holds (pinned and
    /// unpinned) and turn this view into a permanent cache-bypass.
    /// Idempotent; called eagerly from `DB::close`, `DB::drop`, and failed
    /// `DB::open` paths, and automatically when `BlockCache` is dropped. For
    /// a private cache this is a fast cleanup; for a shared pool it releases
    /// the member's capacity promptly instead of waiting for LRU pressure.
    pub fn detach(&self) {
        if self.detached.swap(true, Ordering::SeqCst) {
            return;
        }
        // Pinned sweep — under the update mutex, which excludes racing
        // `insert_pinned` calls (they re-check the flag under the lock).
        {
            let _update = self.pin_updates.lock();
            self.pinned.store(Arc::new(PinnedBlocks::default()));
            self.has_pins.store(false, Ordering::Release);
        }
        // Unpinned sweep: batch every invalidation, then run the pool's
        // maintenance ONCE (per-file `run_pending_tasks` would drain the
        // whole pool's queue repeatedly).
        let files = self.pool.index.take_member(self.member);
        let has_overflow = self.pool.has_overflow.load(Ordering::Acquire);
        let mut any = false;
        for (file_number, offsets) in files {
            for offset in offsets {
                let key = (self.member, file_number, offset);
                self.pool.inner.invalidate(&key);
                if has_overflow {
                    self.pool.overflow.invalidate(&key);
                }
                any = true;
            }
        }
        if any {
            self.pool.inner.run_pending_tasks();
            if has_overflow {
                self.pool.overflow.run_pending_tasks();
            }
        }
    }

    /// Bytes currently pinned by this member.
    pub fn pinned_bytes(&self) -> u64 {
        self.pinned.load().bytes
    }

    /// Approximate entry count across both pool stores for all members, plus
    /// this member's pinned entries. A detached view reports zero.
    pub fn entry_count(&self) -> u64 {
        if self.detached.load(Ordering::Relaxed) {
            return 0;
        }
        self.pool.entry_count() + self.pinned.load().entries.len() as u64
    }
}

impl Drop for BlockCache {
    fn drop(&mut self) {
        self.detach();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn empty_blocks_and_spare_capacity_count_toward_the_limit() {
        let cache = BlockCache::new(1);
        for offset in 0..16 {
            cache.insert(1, offset, Vec::new());
        }
        cache.insert(2, 0, Vec::with_capacity(4096));
        cache.pool.inner.run_pending_tasks();
        assert!(cache.entry_count() <= 1);
        assert!(cache.get(2, 0).is_none());
        let tracked: usize = cache
            .pool
            .index
            .shards
            .iter()
            .map(|shard| shard.lock().values().map(HashSet::len).sum::<usize>())
            .sum();
        assert!(tracked <= 1);
    }

    #[test]
    fn eviction_during_insert_prunes_reverse_offsets() {
        let cache = BlockCache::new(1);
        for offset in 0..1024 {
            // A concurrent maintenance pass can evict an oversized block
            // as soon as insertion makes it visible to moka.
            MAINTAIN_AFTER_INSERT.set(true);
            cache.insert(1, offset, vec![7; 2]);
        }
        cache.pool.inner.run_pending_tasks();
        assert_eq!(cache.entry_count(), 0);
        for shard in &cache.pool.index.shards {
            assert!(shard.lock().is_empty());
        }
    }

    #[test]
    fn test_lru_segments_scale_with_capacity() {
        assert_eq!(lru_segments(0), 1);
        assert_eq!(lru_segments(4096), 1);
        assert_eq!(lru_segments(1024 * 1024), 1);
        assert_eq!(lru_segments(5 * 1024 * 1024), 4);
        assert_eq!(lru_segments(8 * 1024 * 1024), 8);
        assert_eq!(lru_segments(63 * 1024 * 1024), 32);
        assert_eq!(lru_segments(64 * 1024 * 1024), MAX_LRU_SEGMENTS);
        assert_eq!(lru_segments(u64::MAX), MAX_LRU_SEGMENTS);

        for capacity in [5, 6, 31, 63].map(|mib| mib * 1024 * 1024) {
            let segments = lru_segments(capacity);
            assert!(segments.is_power_of_two());
            assert!(
                capacity.div_ceil(segments as u64) >= MIN_LRU_SEGMENT_CAPACITY,
                "capacity {capacity} across {segments} segments fell below the admission floor"
            );
        }
    }

    #[test]
    fn test_cache_capacity_of_one_block_admits_that_block() {
        let cache = BlockCache::new(4096);
        cache.insert(1, 0, vec![0xAB; 4096]);
        cache.pool.inner.run_pending_tasks();

        assert_eq!(*cache.get(1, 0).unwrap(), vec![0xAB; 4096]);
    }

    #[test]
    fn test_block_cache_basic() {
        let cache = BlockCache::new(1024 * 1024); // 1MB

        // Insert
        cache.insert(1, 0, vec![1, 2, 3]);
        cache.insert(1, 4096, vec![4, 5, 6]);
        cache.insert(2, 0, vec![7, 8, 9]);

        // Lookup
        assert_eq!(*cache.get(1, 0).unwrap(), vec![1, 2, 3]);
        assert_eq!(*cache.get(1, 4096).unwrap(), vec![4, 5, 6]);
        assert_eq!(*cache.get(2, 0).unwrap(), vec![7, 8, 9]);

        // Miss
        assert!(cache.get(3, 0).is_none());
    }

    #[test]
    fn test_block_cache_pinned_fast_path() {
        let cache = BlockCache::new(1024 * 1024);

        // An empty pin snapshot still misses cleanly.
        assert_eq!(cache.pinned.load().entries.len(), 0);
        assert!(cache.get(1, 0).is_none());

        // Publishing a pin makes it immediately visible via get().
        cache.insert_pinned(1, 0, vec![9, 9, 9]);
        assert_eq!(cache.pinned.load().entries.len(), 1);
        assert_eq!(*cache.get(1, 0).unwrap(), vec![9, 9, 9]);

        // Re-pinning the same key must not double-count.
        cache.insert_pinned(1, 0, vec![9, 9, 9]);
        assert_eq!(cache.pinned.load().entries.len(), 1);

        // A normal (non-pinned) insert for another key does not change the pins,
        // and is still reachable once the fast path falls through to moka.
        cache.insert(2, 0, vec![1, 2, 3]);
        assert_eq!(cache.pinned.load().entries.len(), 1);
        assert_eq!(*cache.get(2, 0).unwrap(), vec![1, 2, 3]);

        // Unpinning publishes an empty snapshot; pinned entries are never
        // mirrored into the moka-backed store, so the lookup now misses entirely.
        cache.unpin_file(1);
        assert_eq!(cache.pinned.load().entries.len(), 0);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn pinned_and_lru_reads_do_not_wait_for_pin_updates() {
        use std::{sync::mpsc, time::Duration};

        let cache = BlockCache::new(1024 * 1024);
        cache.insert_pinned(1, 0, vec![1]);
        cache.insert(2, 0, vec![2]);
        let update = cache.pin_updates.lock();
        thread::scope(|scope| {
            let (tx, rx) = mpsc::channel();
            let cache = &cache;
            scope.spawn(move || {
                let result = (cache.get(1, 0), cache.get(2, 0));
                tx.send(result).unwrap();
            });
            let result = rx.recv_timeout(Duration::from_secs(5));
            // Release before asserting so a regression cannot hang scope join.
            drop(update);
            let (pinned, lru) = result.expect("read waited for pin update lock");
            assert_eq!(pinned.as_deref(), Some(&vec![1]));
            assert_eq!(lru.as_deref(), Some(&vec![2]));
        });
    }

    #[test]
    fn concurrent_pin_updates_preserve_other_files_and_retired_blocks() {
        use std::sync::Barrier;

        let cache = BlockCache::new(1024 * 1024);
        cache.insert_pinned(0, 0, vec![0]);
        let retired = cache.pinned.load_full();
        let barrier = Barrier::new(9);
        thread::scope(|scope| {
            for file in 1..=8 {
                let (cache, barrier) = (&cache, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    cache.insert_pinned(file, 0, vec![file as u8; file as usize]);
                });
            }
            barrier.wait();
            cache.unpin_file(0);
        });
        assert!(cache.get(0, 0).is_none());
        assert_eq!(
            retired.entries.get(&(0, 0)).unwrap().0.raw_arc().as_slice(),
            &[0]
        );
        for file in 1..=8 {
            assert_eq!(
                cache.get(file, 0).unwrap().as_slice(),
                vec![file as u8; file as usize]
            );
        }
        assert_eq!(cache.pinned.load().entries.len(), 8);
        assert_eq!(cache.pinned_bytes(), 36);

        let barrier = Barrier::new(9);
        thread::scope(|scope| {
            for file in 1..=8 {
                let (cache, barrier) = (&cache, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    cache.insert_pinned(file, 0, vec![9]);
                    cache.unpin_file(file);
                });
            }
            barrier.wait();
            cache.detach();
        });
        assert!(cache.pinned.load().entries.is_empty());
        assert_eq!(cache.pinned_bytes(), 0);
    }

    #[test]
    fn test_block_cache_unpin_file_multiple_pinned_entries() {
        let cache = BlockCache::new(1024 * 1024);
        cache.insert_pinned(1, 0, vec![1]);
        cache.insert_pinned(1, 4096, vec![2]);
        cache.insert_pinned(2, 0, vec![3]);
        assert_eq!(cache.pinned.load().entries.len(), 3);

        // Unpinning one file must decrement by exactly the number of entries
        // removed for that file, leaving other files' pinned entries intact.
        cache.unpin_file(1);
        assert_eq!(cache.pinned.load().entries.len(), 1);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(1, 4096).is_none());
        assert_eq!(*cache.get(2, 0).unwrap(), vec![3]);
    }

    #[test]
    fn test_pool_members_never_collide() {
        // The wrong-data guard: two members both own "file 5, offset 0"
        // with different contents; each must read back exactly its own,
        // for unpinned and pinned entries alike.
        let pool = Arc::new(BlockCachePool::new(1024 * 1024));
        let a = pool.attach();
        let b = pool.attach();

        a.insert(5, 0, vec![0xAA]);
        b.insert(5, 0, vec![0xBB]);
        assert_eq!(*a.get(5, 0).unwrap(), vec![0xAA]);
        assert_eq!(*b.get(5, 0).unwrap(), vec![0xBB]);

        a.insert_pinned(6, 0, vec![0xA1]);
        b.insert_pinned(6, 0, vec![0xB1]);
        assert_eq!(*a.get(6, 0).unwrap(), vec![0xA1]);
        assert_eq!(*b.get(6, 0).unwrap(), vec![0xB1]);

        // Per-member file invalidation must not cross members.
        a.invalidate_file(5);
        assert!(a.get(5, 0).is_none());
        assert_eq!(*b.get(5, 0).unwrap(), vec![0xBB]);
        a.unpin_file(6);
        assert!(a.get(6, 0).is_none());
        assert_eq!(*b.get(6, 0).unwrap(), vec![0xB1]);
    }

    #[test]
    fn test_pool_detach_sweeps_only_the_member() {
        let pool = Arc::new(BlockCachePool::new(1024 * 1024));
        let a = pool.attach();
        let b = pool.attach();

        a.insert(1, 0, vec![1]);
        a.insert(2, 0, vec![2]);
        a.insert_pinned(3, 0, vec![3]);
        b.insert(1, 0, vec![9]);
        b.insert_pinned(3, 0, vec![8]);

        a.detach();

        // Every entry of `a` is gone — pinned included.
        assert!(a.get(1, 0).is_none());
        assert!(a.get(2, 0).is_none());
        assert!(a.get(3, 0).is_none());
        assert_eq!(a.pinned_bytes(), 0);
        assert_eq!(a.entry_count(), 0);

        // `b` is untouched.
        assert_eq!(*b.get(1, 0).unwrap(), vec![9]);
        assert_eq!(*b.get(3, 0).unwrap(), vec![8]);
        assert_eq!(b.pinned.load().entries.len(), 1);

        // The detached view is a permanent cache-bypass.
        let arc = a.insert(4, 0, vec![4]);
        assert_eq!(*arc, vec![4]); // data still returned to the caller
        assert!(a.get(4, 0).is_none());
        let arc = a.insert_pinned(5, 0, vec![5]);
        assert_eq!(*arc, vec![5]);
        assert!(a.get(5, 0).is_none());
        assert_eq!(a.pinned.load().entries.len(), 0); // no leak

        // Idempotent.
        a.detach();
        assert_eq!(*b.get(1, 0).unwrap(), vec![9]);
    }

    #[test]
    fn test_pinned_bytes_accounting() {
        let cache = BlockCache::new(1024 * 1024);
        assert_eq!(cache.pinned_bytes(), 0);

        cache.insert_pinned(1, 0, vec![0; 100]);
        assert_eq!(cache.pinned_bytes(), 100);

        // Replacement adjusts by the delta, both directions.
        cache.insert_pinned(1, 0, vec![0; 150]);
        assert_eq!(cache.pinned_bytes(), 150);
        cache.insert_pinned(1, 0, vec![0; 60]);
        assert_eq!(cache.pinned_bytes(), 60);

        cache.insert_pinned(2, 0, vec![0; 40]);
        assert_eq!(cache.pinned_bytes(), 100);

        cache.unpin_file(1);
        assert_eq!(cache.pinned_bytes(), 40);
        cache.unpin_file(2);
        assert_eq!(cache.pinned_bytes(), 0);
    }

    #[test]
    fn test_disabled_pool_disables_every_member() {
        let pool = Arc::new(BlockCachePool::new(0));
        let a = pool.attach();
        let arc = a.insert(1, 0, vec![1, 2, 3]);
        assert_eq!(*arc, vec![1, 2, 3]); // passthrough
        assert!(a.get(1, 0).is_none());
        let arc = a.insert_pinned(1, 0, vec![4]);
        assert_eq!(*arc, vec![4]);
        assert!(a.get(1, 0).is_none());
        assert_eq!(a.entry_count(), 0);
        assert_eq!(
            pool.usage(),
            BlockCachePoolUsage {
                segmented_capacity_bytes: 0,
                overflow_capacity_bytes: 0,
                segmented_weighted_bytes: 0,
                overflow_weighted_bytes: 0,
            }
        );
    }

    #[test]
    fn oversized_block_above_segment_share_is_retained_and_invalidated() {
        // Default 64 MiB pool splits `inner` into 64 segments (1 MiB each).
        // A 2 MiB block must still be retained via the pool overflow store.
        let pool = Arc::new(BlockCachePool::new(64 * 1024 * 1024));
        let cache = pool.attach();
        cache.insert(7, 0, vec![0xCD; 2 * 1024 * 1024]);
        pool.run_pending_tasks();
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(cache.get(7, 0).unwrap().len(), 2 * 1024 * 1024);

        cache.invalidate_file(7);
        assert!(cache.get(7, 0).is_none());
        assert_eq!(cache.entry_count(), 0);

        cache.insert(8, 0, vec![0xEF; 2 * 1024 * 1024]);
        pool.run_pending_tasks();
        assert_eq!(cache.entry_count(), 1);
        cache.detach();
        assert!(cache.get(8, 0).is_none());
        assert_eq!(pool.entry_count(), 0);
    }

    #[test]
    fn first_overflow_insert_racing_invalidate_file_is_not_stranded() {
        let pool = Arc::new(BlockCachePool::new(64 * 1024 * 1024));
        let cache = Arc::new(pool.attach());
        let cache_for_hook = Arc::clone(&cache);
        BEFORE_TAKE_FILE.with(|cell| {
            *cell.borrow_mut() = Some(Box::new(move || {
                cache_for_hook.insert(7, 0, vec![0xAB; 2 * 1024 * 1024]);
            }));
        });

        cache.invalidate_file(7);
        let hit_after_invalidate = cache.get(7, 0).is_some();

        cache.detach();
        pool.run_pending_tasks();
        assert!(
            !hit_after_invalidate,
            "invalidate_file left first oversized block readable"
        );
        assert_eq!(pool.entry_count(), 0);
        assert_eq!(pool.usage().overflow_weighted_bytes, 0);
    }
}
