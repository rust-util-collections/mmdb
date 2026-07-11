//! LRU block cache for SST data blocks, backed by moka.
//!
//! Two-level shape: a [`BlockCachePool`] owns the shared storage (moka
//! LRU + sharded reverse index) and can be shared by many DBs; a
//! [`BlockCache`] is one DB's *member view* of a pool (and privately
//! owns that member's pinned entries). Every pool key is namespaced by
//! a pool-unique member id, so two DBs that both own an SST numbered 5
//! can never observe each other's blocks — sharing is a capacity
//! decision, never a correctness one.
//!
//! The historical single-DB API is unchanged: [`BlockCache::new`]
//! builds a private single-member pool internally, so callers that
//! never share see exactly the old behavior.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use moka::notification::RemovalCause;
use parking_lot::Mutex;

/// Pool cache key: (member_id, sst_file_number, block_offset).
type CacheKey = (u64, u64, u64);

/// Cached block data.
type CacheValue = Arc<Vec<u8>>;

/// Number of shards in the reverse index. Must be a power of two.
///
/// With a single-mutex index, every insert (the read-miss path) and
/// every eviction-listener callback of every member DB would serialize
/// on one lock; sharding bounds that fan-in.
const FO_SHARDS: usize = 16;

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
type FoShard = Mutex<HashMap<(u64, u64), HashSet<u64>>>;

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
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
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

    /// Eviction-listener path: drop one offset, pruning empty file sets.
    fn remove(&self, member: u64, file_number: u64, block_offset: u64) {
        let mut map = self.shards[Self::shard_for(member, file_number)].lock();
        if let Some(set) = map.get_mut(&(member, file_number)) {
            set.remove(&block_offset);
            if set.is_empty() {
                map.remove(&(member, file_number));
            }
        }
    }

    /// Remove and return every tracked offset of one (member, file).
    fn take_file(&self, member: u64, file_number: u64) -> Option<HashSet<u64>> {
        self.shards[Self::shard_for(member, file_number)]
            .lock()
            .remove(&(member, file_number))
    }

    /// Remove and return every tracked (file, offsets) of one member —
    /// the detach sweep. O(total index entries), acceptable: detach is
    /// a close-time event.
    fn take_member(&self, member: u64) -> Vec<(u64, HashSet<u64>)> {
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

/// Shared storage for one or more DBs' block caches: a single moka LRU
/// (capacity applies to the whole pool) and the sharded reverse index.
/// Pinned entries live on each member's [`BlockCache`] view, not here.
/// Construct once, wrap in an `Arc`, and hand a
/// [`attach`](Self::attach)ed view to each DB (via
/// `DbOptions::block_cache`); DBs given the same pool share capacity,
/// DBs given none keep a private pool. The LRU is segmented, so a single
/// unpinned block must fit the capacity share of its hashed segment; caches
/// below 1 MiB use one segment, while pools of 64 MiB or more use 64.
pub struct BlockCachePool {
    inner: moka::sync::SegmentedCache<CacheKey, CacheValue>,
    /// Track which offsets belong to each (member, file) for bulk invalidation.
    index: Arc<FileOffsetsIndex>,
    /// Member-id allocator for [`attach`](Self::attach).
    next_member: AtomicU64,
    /// When true (capacity 0), caching is disabled: inserts are no-ops and
    /// lookups always miss. Honors the documented "0 disables caching" option.
    disabled: bool,
}

impl BlockCachePool {
    /// Create a new pool with the given capacity in bytes (the capacity
    /// bounds the pool as a whole, not any single member).
    /// A capacity of 0 disables caching entirely.
    pub fn new(capacity_bytes: u64) -> Self {
        let index = Arc::new(FileOffsetsIndex::new());
        let listener_index = index.clone();
        let inner = moka::sync::SegmentedCache::builder(lru_segments(capacity_bytes))
            .max_capacity(capacity_bytes)
            .weigher(|_key: &CacheKey, value: &CacheValue| -> u32 {
                value.len().min(u32::MAX as usize) as u32
            })
            .eviction_listener(
                move |key: Arc<CacheKey>, _value: CacheValue, _cause: RemovalCause| {
                    let (member, file_number, block_offset) = *key;
                    listener_index.remove(member, file_number, block_offset);
                },
            )
            .build();
        Self {
            inner,
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
            pinned: Mutex::new(HashMap::new()),
            pinned_count: AtomicUsize::new(0),
            pinned_bytes: AtomicU64::new(0),
        }
    }

    /// Approximate entry count of the whole pool's LRU store (all
    /// members; pinned entries are member-local and not included).
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

/// One DB's view of a [`BlockCachePool`]: the same five-method surface
/// the cache has always had, with every key transparently namespaced
/// by this member's id.
///
/// Supports pinning entries that should never be evicted — in practice
/// this is just the first data block of each L0 file (see
/// [`insert_pinned`](Self::insert_pinned)).
pub struct BlockCache {
    pool: Arc<BlockCachePool>,
    member: u64,
    /// Set once by [`detach`](Self::detach); afterwards the view is a
    /// cache-bypass (misses on read, caches nothing on write). This is
    /// a cutoff, not a barrier: an in-flight *unpinned* insert racing
    /// the detach sweep may leave a transient orphan entry, which LRU
    /// reclaims like any cold block (its member id is retired, so it
    /// can never be read again). Pinned inserts cannot race the sweep:
    /// both sides serialize on the `pinned` mutex.
    detached: AtomicBool,
    /// Pinned entries — never evicted by LRU. **Member-local** (keyed
    /// `(file_number, block_offset)`): pins are never shared, never
    /// weighed against the pool's capacity, and keeping them here means
    /// one member's pins add zero lock traffic to other members' `get`
    /// fast paths — exactly the per-DB behavior of the pre-pool cache.
    pinned: Mutex<HashMap<(u64, u64), CacheValue>>,
    /// Fast-path hint: number of entries currently in `pinned`. `get()` checks
    /// this atomic before acquiring `pinned`'s mutex so the common case (no
    /// pinned entries at all, or a lookup for a key that isn't one) skips the
    /// lock entirely. `Relaxed` is sufficient since this is only a hint —
    /// `pinned` (guarded by its own mutex) remains the authoritative state and
    /// is always consulted whenever the counter reads nonzero.
    ///
    /// Ordering invariant: the counter is incremented *before* a new entry is
    /// inserted (both under the `pinned` mutex) and decremented only while
    /// holding the mutex after removal. A reader that observes 0 therefore
    /// sees a correct miss ("insert not yet complete"); a reader that observes
    /// nonzero takes the mutex and blocks until the in-flight insert finishes.
    /// Incrementing *after* the insert would leave a window where a concurrent
    /// `get()` skips the map even though the entry is already present.
    pinned_count: AtomicUsize,
    /// Bytes currently pinned by this member (observability; pinning is
    /// structurally small — one block per L0 file).
    pinned_bytes: AtomicU64,
}

impl BlockCache {
    /// Create a private single-member cache with the given capacity in
    /// bytes — the non-shared form, and exactly the historical behavior.
    /// A capacity of 0 disables caching entirely.
    pub fn new(capacity_bytes: u64) -> Self {
        Arc::new(BlockCachePool::new(capacity_bytes)).attach()
    }

    /// Look up a cached block. Pinned entries are checked first.
    pub fn get(&self, file_number: u64, block_offset: u64) -> Option<Arc<Vec<u8>>> {
        if self.pool.disabled || self.detached.load(Ordering::Relaxed) {
            return None;
        }
        // Fast path: `pinned` is empty for the vast majority of lookups (only
        // one data block per L0 file is ever pinned), so skip its mutex
        // entirely unless the hint counter says there's something to find.
        if self.pinned_count.load(Ordering::Relaxed) != 0
            && let Some(v) = self.pinned.lock().get(&(file_number, block_offset))
        {
            return Some(v.clone());
        }
        self.pool
            .inner
            .get(&(self.member, file_number, block_offset))
    }

    /// Insert a block into the cache.
    ///
    /// The moka write and the `index.add()` below are not atomic as a
    /// pair. If a concurrent `invalidate_file()` for this file runs its
    /// index read-and-clear (`take_file`) after the moka write lands but
    /// before `index.add()` runs, the snapshot it reads misses this
    /// offset, so `invalidate_file` never targets it — the block stays
    /// live in moka past its file's removal, reclaimed only by later LRU
    /// pressure. This is bounded, not a correctness bug: SST file numbers
    /// are never reused, so the stray entry can never be misread as data
    /// belonging to a different file, only reclaimed late instead of
    /// promptly. Making the pair atomic would require locking per insert,
    /// reintroducing the contention `FO_SHARDS` sharding exists to avoid,
    /// so this is accepted as-is — a structurally similar cutoff-not-
    /// barrier tradeoff to the one documented on the `detached` field.
    pub fn insert(&self, file_number: u64, block_offset: u64, data: Vec<u8>) -> Arc<Vec<u8>> {
        let arc = Arc::new(data);
        if self.pool.disabled || self.detached.load(Ordering::Relaxed) {
            return arc;
        }
        self.pool
            .inner
            .insert((self.member, file_number, block_offset), arc.clone());
        self.pool.index.add(self.member, file_number, block_offset);
        arc
    }

    /// Insert a pinned block — never evicted by LRU.
    /// Used to pin the first data block (smallest key) of an L0 file, so the
    /// merging iterator's initial `peek()` is always a cache hit. Index and
    /// filter blocks are held directly as fields on `TableReader` and never
    /// pass through this cache at all.
    pub fn insert_pinned(
        &self,
        file_number: u64,
        block_offset: u64,
        data: Vec<u8>,
    ) -> Arc<Vec<u8>> {
        let arc = Arc::new(data);
        if self.pool.disabled {
            return arc;
        }
        {
            let mut pinned = self.pinned.lock();
            // Re-checked under the mutex: `detach` sweeps this map under the
            // same lock, so a pin can never slip in after the sweep (an
            // unremovable leak); see the `detached` field docs.
            if self.detached.load(Ordering::Relaxed) {
                return arc;
            }
            match pinned.get(&(file_number, block_offset)) {
                Some(old) => {
                    // Replacement: adjust the byte counter by the delta.
                    let (old_len, new_len) = (old.len() as u64, arc.len() as u64);
                    if new_len >= old_len {
                        self.pinned_bytes
                            .fetch_add(new_len - old_len, Ordering::Relaxed);
                    } else {
                        self.pinned_bytes
                            .fetch_sub(old_len - new_len, Ordering::Relaxed);
                    }
                }
                None => {
                    // Increment BEFORE inserting a new key (see `pinned_count`
                    // invariant): the lock-free reader in `get()` never takes
                    // this mutex when it reads 0, so counting after the insert
                    // would let it miss an entry that is already in the map.
                    self.pinned_count.fetch_add(1, Ordering::Relaxed);
                    self.pinned_bytes
                        .fetch_add(arc.len() as u64, Ordering::Relaxed);
                }
            }
            pinned.insert((file_number, block_offset), arc.clone());
        }
        arc
    }

    /// Unpin all entries for a specific file (e.g., when L0 file is compacted away).
    pub fn unpin_file(&self, file_number: u64) {
        if self.detached.load(Ordering::Relaxed) {
            return;
        }
        let mut pinned = self.pinned.lock();
        let mut removed = 0usize;
        let mut removed_bytes = 0u64;
        pinned.retain(|&(f, _), v| {
            if f == file_number {
                removed += 1;
                removed_bytes += v.len() as u64;
                false
            } else {
                true
            }
        });
        if removed != 0 {
            self.pinned_count.fetch_sub(removed, Ordering::Relaxed);
            self.pinned_bytes
                .fetch_sub(removed_bytes, Ordering::Relaxed);
        }
    }

    /// Invalidate all cached blocks this member holds for a specific file.
    /// Called after compaction removes an SST file. Other members'
    /// same-numbered files (unrelated DBs) are untouched.
    ///
    /// Reads-and-clears the reverse index (`take_file`) and invalidates
    /// only the offsets that snapshot contains — see `insert()`'s doc for
    /// the narrow race where a concurrently-inserted offset for this file
    /// can land in the index just after that snapshot and be missed here.
    /// Accepted as a bounded, benign tradeoff rather than fixed.
    pub fn invalidate_file(&self, file_number: u64) {
        if self.detached.load(Ordering::Relaxed) {
            return;
        }
        self.unpin_file(file_number);
        if let Some(offsets) = self.pool.index.take_file(self.member, file_number) {
            for offset in offsets {
                self.pool
                    .inner
                    .invalidate(&(self.member, file_number, offset));
            }
            self.pool.inner.run_pending_tasks();
        }
    }

    /// Leave the pool: sweep every entry this member holds (pinned and
    /// unpinned) and turn this view into a permanent cache-bypass.
    /// Idempotent; called from both `DB::close` and `DB::drop` (either
    /// may run first). For a private cache this is a fast no-op-like
    /// cleanup; for a shared pool it releases the member's capacity
    /// promptly instead of waiting for LRU pressure to notice.
    pub fn detach(&self) {
        if self.detached.swap(true, Ordering::SeqCst) {
            return;
        }
        // Pinned sweep — under the pinned mutex, which excludes racing
        // `insert_pinned` calls (they re-check the flag under the lock).
        {
            let mut pinned = self.pinned.lock();
            let removed = pinned.len();
            pinned.clear();
            if removed != 0 {
                self.pinned_count.fetch_sub(removed, Ordering::Relaxed);
            }
        }
        self.pinned_bytes.store(0, Ordering::Relaxed);
        // Unpinned sweep: batch every invalidation, then run the pool's
        // maintenance ONCE (per-file `run_pending_tasks` would drain the
        // whole pool's queue repeatedly).
        let files = self.pool.index.take_member(self.member);
        let mut any = false;
        for (file_number, offsets) in files {
            for offset in offsets {
                self.pool
                    .inner
                    .invalidate(&(self.member, file_number, offset));
                any = true;
            }
        }
        if any {
            self.pool.inner.run_pending_tasks();
        }
    }

    /// Bytes currently pinned by this member.
    pub fn pinned_bytes(&self) -> u64 {
        self.pinned_bytes.load(Ordering::Relaxed)
    }

    /// Current approximate entry count: the underlying pool's LRU store
    /// (all members) plus this member's pinned entries. For a private
    /// (non-shared) cache this is exactly this DB's count — the
    /// historical semantics; a detached view reports 0.
    pub fn entry_count(&self) -> u64 {
        if self.detached.load(Ordering::Relaxed) {
            return 0;
        }
        self.pool.entry_count() + self.pinned.lock().len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Nothing pinned yet: the hint counter is zero, so get() must take the
        // fast path (skip `pinned`'s mutex) and still miss cleanly.
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 0);
        assert!(cache.get(1, 0).is_none());

        // Pinning bumps the counter and is immediately visible via get().
        cache.insert_pinned(1, 0, vec![9, 9, 9]);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 1);
        assert_eq!(*cache.get(1, 0).unwrap(), vec![9, 9, 9]);

        // Re-pinning the same key must not double-count.
        cache.insert_pinned(1, 0, vec![9, 9, 9]);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 1);

        // A normal (non-pinned) insert for another key doesn't touch the counter,
        // and is still reachable once the fast path falls through to moka.
        cache.insert(2, 0, vec![1, 2, 3]);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 1);
        assert_eq!(*cache.get(2, 0).unwrap(), vec![1, 2, 3]);

        // Unpinning drops the counter back to zero; pinned entries are never
        // mirrored into the moka-backed store, so the lookup now misses entirely.
        cache.unpin_file(1);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 0);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn test_block_cache_unpin_file_multiple_pinned_entries() {
        let cache = BlockCache::new(1024 * 1024);
        cache.insert_pinned(1, 0, vec![1]);
        cache.insert_pinned(1, 4096, vec![2]);
        cache.insert_pinned(2, 0, vec![3]);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 3);

        // Unpinning one file must decrement by exactly the number of entries
        // removed for that file, leaving other files' pinned entries intact.
        cache.unpin_file(1);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 1);
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
        assert_eq!(b.pinned_count.load(Ordering::Relaxed), 1);

        // The detached view is a permanent cache-bypass.
        let arc = a.insert(4, 0, vec![4]);
        assert_eq!(*arc, vec![4]); // data still returned to the caller
        assert!(a.get(4, 0).is_none());
        let arc = a.insert_pinned(5, 0, vec![5]);
        assert_eq!(*arc, vec![5]);
        assert!(a.get(5, 0).is_none());
        assert_eq!(a.pinned_count.load(Ordering::Relaxed), 0); // no leak

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
    }
}
