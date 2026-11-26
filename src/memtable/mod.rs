//! MemTable: in-memory sorted key-value store backed by a lock-free skiplist.

#[cfg(any(test, all(miri, feature = "test-utils")))]
pub(crate) mod arena_tests;
pub mod skiplist;
pub mod skiplist_impl;

pub use skiplist::SkipListMemTable;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{
    ptr::NonNull,
    sync::{Arc, OnceLock},
};

use arc_swap::ArcSwapOption;
use parking_lot::RwLock;

use crate::iterator::range_del::FragmentedRangeTombstoneList;

#[cfg(test)]
use crate::types::InternalKey;
use crate::types::{SequenceNumber, ValueType};

const TOMBSTONE_TAIL_CAP: usize = 8;

/// A cached range tombstone from a memtable.
pub struct MemRangeTombstone {
    pub begin: Arc<[u8]>,
    pub end: Arc<[u8]>,
    pub seq: SequenceNumber,
}

type RawTombstone = (Arc<[u8]>, Arc<[u8]>, SequenceNumber);

#[derive(Clone)]
struct TombstoneTier {
    count: usize,
    min_begin: Arc<[u8]>,
    max_end: Arc<[u8]>,
    max_seq: SequenceNumber,
    retained_bytes: usize,
    raw: Arc<Vec<RawTombstone>>,
    index: Arc<FragmentedRangeTombstoneList>,
}

/// Lock-free logarithmic Bentley-Saxe index of active-memtable range tombstones.
/// Point lookups skip disjoint ranges in O(1) via summary bounds and query
/// powers-of-two `FragmentedRangeTombstoneList` tiers in O(log³ T) worst-case
/// without taking `range_tombstones`'s `RwLock`. Endpoint bytes are shared via
/// `Arc<[u8]>` across `MemRangeTombstone`, summary bounds, tail, and tiers so
/// inserting a small range after a large-endpoint range copies zero payload bytes.
#[derive(Clone)]
struct ActiveTombstoneIndex {
    min_begin: Arc<[u8]>,
    max_end: Arc<[u8]>,
    max_seq: SequenceNumber,
    tiers: Vec<TombstoneTier>,
    tail: Vec<RawTombstone>,
    tail_min_begin: Arc<[u8]>,
    tail_max_end: Arc<[u8]>,
    tail_max_seq: SequenceNumber,
}

impl Default for ActiveTombstoneIndex {
    fn default() -> Self {
        let empty: Arc<[u8]> = Arc::from(&[][..]);
        Self {
            min_begin: Arc::clone(&empty),
            max_end: Arc::clone(&empty),
            max_seq: 0,
            tiers: Vec::new(),
            tail: Vec::new(),
            tail_min_begin: Arc::clone(&empty),
            tail_max_end: empty,
            tail_max_seq: 0,
        }
    }
}

impl ActiveTombstoneIndex {
    /// Structural heap bytes owned by this index (excluding shared `Arc<[u8]>`
    /// endpoint payloads already charged once in `approximate_size`).
    fn retained_bytes(&self) -> usize {
        self.tiers.capacity() * size_of::<TombstoneTier>()
            + self.tail.capacity() * size_of::<RawTombstone>()
            + self
                .tiers
                .iter()
                .map(|t| t.retained_bytes)
                .fold(0usize, usize::saturating_add)
    }

    fn append(&mut self, begin: Arc<[u8]>, end: Arc<[u8]>, seq: SequenceNumber) {
        if self.tiers.is_empty() && self.tail.is_empty() {
            self.min_begin = Arc::clone(&begin);
            self.max_end = Arc::clone(&end);
            self.max_seq = seq;
        } else {
            if begin.as_ref() < self.min_begin.as_ref() {
                self.min_begin = Arc::clone(&begin);
            }
            if end.as_ref() > self.max_end.as_ref() {
                self.max_end = Arc::clone(&end);
            }
            if seq > self.max_seq {
                self.max_seq = seq;
            }
        }

        if self.tail.is_empty() {
            self.tail_min_begin = Arc::clone(&begin);
            self.tail_max_end = Arc::clone(&end);
            self.tail_max_seq = seq;
        } else {
            if begin.as_ref() < self.tail_min_begin.as_ref() {
                self.tail_min_begin = Arc::clone(&begin);
            }
            if end.as_ref() > self.tail_max_end.as_ref() {
                self.tail_max_end = Arc::clone(&end);
            }
            if seq > self.tail_max_seq {
                self.tail_max_seq = seq;
            }
        }
        self.tail.push((begin, end, seq));

        if self.tail.len() >= TOMBSTONE_TAIL_CAP {
            let empty: Arc<[u8]> = Arc::from(&[][..]);
            let mut min_begin = std::mem::replace(&mut self.tail_min_begin, Arc::clone(&empty));
            let mut max_end = std::mem::replace(&mut self.tail_max_end, empty);
            let mut max_seq = self.tail_max_seq;
            self.tail_max_seq = 0;
            let mut merged = std::mem::take(&mut self.tail);
            while self.tiers.last().is_some_and(|t| t.count == merged.len()) {
                let prev = self.tiers.pop().unwrap();
                if prev.min_begin.as_ref() < min_begin.as_ref() {
                    min_begin = prev.min_begin;
                }
                if prev.max_end.as_ref() > max_end.as_ref() {
                    max_end = prev.max_end;
                }
                if prev.max_seq > max_seq {
                    max_seq = prev.max_seq;
                }
                let mut combined = Vec::with_capacity(prev.raw.len() + merged.len());
                combined.extend(prev.raw.iter().cloned());
                combined.append(&mut merged);
                merged = combined;
            }
            let count = merged.len();
            let index = Arc::new(FragmentedRangeTombstoneList::from_shared(&merged));
            let retained_bytes =
                merged.capacity() * size_of::<RawTombstone>() + index.approximate_heap_bytes();
            self.tiers.push(TombstoneTier {
                count,
                min_begin,
                max_end,
                max_seq,
                retained_bytes,
                raw: Arc::new(merged),
                index,
            });
        }
    }

    fn max_covering_tombstone_seq(
        &self,
        user_key: &[u8],
        read_seq: SequenceNumber,
    ) -> SequenceNumber {
        if (self.tiers.is_empty() && self.tail.is_empty())
            || user_key < self.min_begin.as_ref()
            || user_key >= self.max_end.as_ref()
        {
            return 0;
        }
        let mut best: SequenceNumber = 0;
        if !self.tail.is_empty()
            && self.tail_max_seq > best
            && user_key >= self.tail_min_begin.as_ref()
            && user_key < self.tail_max_end.as_ref()
        {
            for (begin, end, seq) in &self.tail {
                if *seq <= read_seq
                    && *seq > best
                    && user_key >= begin.as_ref()
                    && user_key < end.as_ref()
                {
                    best = *seq;
                }
            }
        }
        for tier in self.tiers.iter().rev() {
            if tier.max_seq <= best
                || user_key < tier.min_begin.as_ref()
                || user_key >= tier.max_end.as_ref()
            {
                continue;
            }
            let seq = tier.index.max_covering_tombstone_seq(user_key, read_seq);
            if seq > best {
                best = seq;
            }
        }
        best
    }

    fn overlaps_user_key_range(&self, min_key: &[u8], max_key: &[u8]) -> bool {
        if (self.tiers.is_empty() && self.tail.is_empty())
            || self.min_begin.as_ref() > max_key
            || self.max_end.as_ref() <= min_key
        {
            return false;
        }
        if !self.tail.is_empty()
            && self.tail_min_begin.as_ref() <= max_key
            && self.tail_max_end.as_ref() > min_key
            && self
                .tail
                .iter()
                .any(|(b, e, _)| b.as_ref() <= max_key && e.as_ref() > min_key)
        {
            return true;
        }
        for tier in &self.tiers {
            if tier.min_begin.as_ref() <= max_key
                && tier.max_end.as_ref() > min_key
                && tier
                    .raw
                    .iter()
                    .any(|(b, e, _)| b.as_ref() <= max_key && e.as_ref() > min_key)
            {
                return true;
            }
        }
        false
    }
}

/// A MemTable stores recent writes in memory before they are flushed to SST.
///
/// Keys are InternalKey-encoded (user_key + seq + type), values are raw bytes.
/// The skiplist sorts by `compare_internal_key` ordering.
pub struct MemTable {
    inner: SkipListMemTable,
    // Upper bounds for groups of keys; a collision only triggers an exact
    // skiplist run lookup. No per-key heap index is needed by the memtable.
    metadata_buckets: [AtomicUsize; 64],
    max_metadata_bucket: AtomicUsize,
    /// Approximate memory usage in bytes (arena slabs + raw range-tombstone payloads).
    approximate_size: AtomicUsize,
    /// Net structural heap bytes owned by the active or frozen range-tombstone index.
    tombstone_index_bytes: AtomicUsize,
    /// Whether this memtable contains any RangeDeletion entries.
    /// Used to skip range tombstone checks when false.
    has_range_deletions: AtomicBool,
    /// Raw collection of range tombstones retained for flush and scan construction.
    range_tombstones: RwLock<Vec<MemRangeTombstone>>,
    /// Lock-free tiered interval index for active-memtable point lookups.
    /// Empty (`None`) until the first valid `RangeDeletion` is inserted.
    active_tombstones: ArcSwapOption<ActiveTombstoneIndex>,
    /// Set when the memtable is frozen for flush; no puts follow.
    immutable: AtomicBool,
    /// Fragmented index of `range_tombstones`, built on the first coverage
    /// check after freeze: O(log T) lookups that take no lock.
    frozen_tombstones: OnceLock<FragmentedRangeTombstoneList>,
}

/// An immutable slice in a slab retained by its owning memtable.
#[derive(Clone)]
pub(crate) struct MemTablePin {
    _owner: Arc<MemTable>,
    bytes: NonNull<[u8]>,
}

// SAFETY: the slice is initialized before release publication, never mutated
// or reused, and its slab lives until the last owning Arc is dropped.
unsafe impl Send for MemTablePin {}
// SAFETY: shared readers can only observe the immutable bytes above.
unsafe impl Sync for MemTablePin {}

impl MemTablePin {
    #[inline]
    pub(crate) fn from_raw(owner: &Arc<MemTable>, bytes: NonNull<[u8]>) -> Self {
        Self {
            _owner: owner.clone(),
            bytes,
        }
    }

    #[inline]
    pub(crate) fn update_if_same_owner(
        &mut self,
        owner: &Arc<MemTable>,
        bytes: NonNull<[u8]>,
    ) -> bool {
        if Arc::ptr_eq(&self._owner, owner) {
            self.bytes = bytes;
            true
        } else {
            false
        }
    }
}

impl AsRef<[u8]> for MemTablePin {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: _owner retains this node, including zero-length values.
        unsafe { self.bytes.as_ref() }
    }
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            inner: SkipListMemTable::new(),
            metadata_buckets: std::array::from_fn(|_| AtomicUsize::new(0)),
            max_metadata_bucket: AtomicUsize::new(0),
            approximate_size: AtomicUsize::new(0),
            tombstone_index_bytes: AtomicUsize::new(0),
            has_range_deletions: AtomicBool::new(false),
            range_tombstones: RwLock::new(Vec::new()),
            active_tombstones: ArcSwapOption::const_empty(),
            immutable: AtomicBool::new(false),
            frozen_tombstones: OnceLock::new(),
        }
    }

    /// Mark the memtable immutable. Called when it is frozen for flush;
    /// the caller must not insert into it afterwards.
    pub fn mark_immutable(&self) {
        self.immutable.store(true, Ordering::Release);
    }

    /// Insert an entry. `key` is the user key; it will be encoded as an InternalKey.
    pub fn put(&self, key: &[u8], value: &[u8], sequence: SequenceNumber, value_type: ValueType) {
        // An empty or inverted range deletion `[begin, end)` with `begin >= end`
        // covers no keys. Storing it would leave a RangeDeletion entry keyed at
        // `begin` that a point lookup misreads as a deletion of `begin`, so treat
        // it as a no-op. (`value` holds the range's end key for RangeDeletion.)
        if value_type == ValueType::RangeDeletion && key >= value {
            return;
        }
        let mut allocated = 0;
        if value_type == ValueType::RangeDeletion {
            let mut ranges = self.range_tombstones.write();
            let old_cap = ranges.capacity();
            let begin: Arc<[u8]> = Arc::from(key);
            let end: Arc<[u8]> = Arc::from(value);
            ranges.push(MemRangeTombstone {
                begin: Arc::clone(&begin),
                end: Arc::clone(&end),
                seq: sequence,
            });
            let mut next_idx = self
                .active_tombstones
                .load()
                .as_deref()
                .cloned()
                .unwrap_or_default();
            next_idx.append(begin, end, sequence);
            let idx_bytes = next_idx.retained_bytes();
            self.active_tombstones.store(Some(Arc::new(next_idx)));
            self.tombstone_index_bytes
                .store(idx_bytes, Ordering::Relaxed);
            allocated += (ranges.capacity() - old_cap) * size_of::<MemRangeTombstone>()
                + key.len()
                + value.len();
            self.has_range_deletions.store(true, Ordering::Release);
        }
        let value = if value_type == ValueType::Deletion {
            &[]
        } else {
            value
        };
        let charge = crate::types::BatchEntry {
            key,
            value,
            value_type,
        }
        .metadata_charge();
        let bucket = &self.metadata_buckets[Self::metadata_bucket(key)];
        let new_bucket = bucket.fetch_add(charge, Ordering::Relaxed) + charge;
        if new_bucket > self.max_metadata_bucket.load(Ordering::Relaxed) {
            self.max_metadata_bucket
                .fetch_max(new_bucket, Ordering::Relaxed);
        }
        allocated += self.inner.insert_entry(key, sequence, value_type, value);
        self.approximate_size
            .fetch_add(allocated, Ordering::Relaxed);
    }

    /// Insert an entire validated `WriteBatch` starting at `first_seq`, holding
    /// the skiplist arena guard across contiguous point entries and publishing
    /// `approximate_size` once per batch. Returns total user key+value bytes.
    pub(crate) fn put_batch(
        &self,
        batch: &crate::types::WriteBatch,
        first_seq: SequenceNumber,
    ) -> u64 {
        let mut total_bytes = 0u64;
        let mut total_allocated = 0usize;
        let mut max_bucket_seen = self.max_metadata_bucket.load(Ordering::Relaxed);
        let mut arena_guard = Some(self.inner.lock_arena());

        for (i, entry) in batch.entries().enumerate() {
            let seq = first_seq + i as u64;
            let key = entry.key;
            let raw_value = entry.value;
            let value_type = entry.value_type;
            total_bytes += key.len() as u64 + raw_value.len() as u64;

            if value_type == ValueType::RangeDeletion {
                if key >= raw_value {
                    continue;
                }
                // Release `arena` before taking `range_tombstones` so the two
                // internal locks never nest.
                drop(arena_guard.take());
                {
                    let mut ranges = self.range_tombstones.write();
                    let old_cap = ranges.capacity();
                    let begin: Arc<[u8]> = Arc::from(key);
                    let end: Arc<[u8]> = Arc::from(raw_value);
                    ranges.push(MemRangeTombstone {
                        begin: Arc::clone(&begin),
                        end: Arc::clone(&end),
                        seq,
                    });
                    let mut next_idx = self
                        .active_tombstones
                        .load()
                        .as_deref()
                        .cloned()
                        .unwrap_or_default();
                    next_idx.append(begin, end, seq);
                    let idx_bytes = next_idx.retained_bytes();
                    self.active_tombstones.store(Some(Arc::new(next_idx)));
                    self.tombstone_index_bytes
                        .store(idx_bytes, Ordering::Relaxed);
                    total_allocated += (ranges.capacity() - old_cap)
                        * size_of::<MemRangeTombstone>()
                        + key.len()
                        + raw_value.len();
                    self.has_range_deletions.store(true, Ordering::Release);
                }
                arena_guard = Some(self.inner.lock_arena());
            }

            let value = if value_type == ValueType::Deletion {
                &[]
            } else {
                raw_value
            };
            let charge = crate::types::BatchEntry {
                key,
                value,
                value_type,
            }
            .metadata_charge();
            let bucket = &self.metadata_buckets[Self::metadata_bucket(key)];
            let new_bucket = bucket.fetch_add(charge, Ordering::Relaxed) + charge;
            if new_bucket > max_bucket_seen {
                max_bucket_seen = new_bucket;
            }
            total_allocated += self.inner.insert_entry_locked(
                arena_guard.as_mut().unwrap(),
                key,
                seq,
                value_type,
                value,
            );
        }
        drop(arena_guard);

        if max_bucket_seen > self.max_metadata_bucket.load(Ordering::Relaxed) {
            self.max_metadata_bucket
                .fetch_max(max_bucket_seen, Ordering::Relaxed);
        }
        if total_allocated != 0 {
            self.approximate_size
                .fetch_add(total_allocated, Ordering::Relaxed);
        }
        total_bytes
    }

    #[inline]
    fn metadata_bucket(key: &[u8]) -> usize {
        // Fast bounded-sample 64-bit mix (no SipHash state setup). Collisions
        // are resolved exactly by `same_key_metadata`.
        let len = key.len();
        let mut head = 0u64;
        let mut tail = 0u64;
        if len >= 8 {
            head = u64::from_le_bytes(key[..8].try_into().unwrap());
            tail = u64::from_le_bytes(key[len - 8..].try_into().unwrap());
        } else {
            for (i, &b) in key.iter().enumerate() {
                head |= (b as u64) << (i * 8);
            }
        }
        let mut h = (len as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        h = (h ^ head).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        h = (h ^ tail.rotate_left(31)).wrapping_mul(0x94d0_49bb_1331_11eb);
        ((h >> 58) ^ h) as usize & 63
    }

    pub(crate) fn max_metadata_charge(&self) -> usize {
        self.max_metadata_bucket.load(Ordering::Relaxed)
    }

    pub(crate) fn metadata_charge_bound(&self, key: &[u8]) -> usize {
        self.metadata_buckets[Self::metadata_bucket(key)].load(Ordering::Relaxed)
    }

    pub(crate) fn same_key_metadata(&self, key: &[u8]) -> usize {
        self.inner.same_key_metadata(key)
    }

    pub(crate) fn point_value<V: crate::pinned::PointValue>(
        self: &Arc<Self>,
        key: &[u8],
        sequence: SequenceNumber,
        covering_tombstone_seq: SequenceNumber,
    ) -> Option<(Option<V>, SequenceNumber)> {
        let (value, seq) = self.inner.get_ref_with_seq(key, sequence)?;
        let projected = if covering_tombstone_seq > seq {
            None
        } else {
            value.map(|value| {
                V::project(value, || {
                    crate::PinnedSlice::from_memtable(MemTablePin {
                        _owner: self.clone(),
                        bytes: NonNull::from(value),
                    })
                })
            })
        };
        Some((projected, seq))
    }

    /// Look up a user key at or below the given sequence number.
    /// Returns `Some(Some(value))` for a found value, `Some(None)` for a deletion tombstone,
    /// or `None` if the key is not in this MemTable.
    #[cfg(test)]
    pub fn get(&self, key: &[u8], sequence: SequenceNumber) -> Option<Option<Vec<u8>>> {
        self.get_with_seq(key, sequence).map(|(result, _)| result)
    }

    /// Like `get`, but also returns the sequence number of the found entry.
    #[cfg(test)]
    pub fn get_with_seq(
        &self,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Option<(Option<Vec<u8>>, SequenceNumber)> {
        self.inner
            .get_ref_with_seq(key, sequence)
            .map(|(v, seq)| (v.map(<[u8]>::to_vec), seq))
    }

    /// Approximate memory usage in bytes.
    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(Ordering::Relaxed)
            .saturating_add(self.tombstone_index_bytes.load(Ordering::Relaxed))
    }

    /// Return an iterator over all entries in order.
    /// Each item is (encoded_internal_key, value).
    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        self.inner.iter()
    }

    /// Borrowing iteration over all entries in order, without copying them.
    /// Each item is (encoded_internal_key, value).
    pub fn iter_ref(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.inner.iter_ref()
    }

    /// Whether this memtable contains any range deletion entries.
    pub fn has_range_deletions(&self) -> bool {
        // Acquire: pairs with the Release store in put() to ensure
        // visibility across cores on weakly-ordered architectures (ARM/RISC-V).
        self.has_range_deletions.load(Ordering::Acquire)
    }

    fn init_frozen_tombstones(&self) -> &FragmentedRangeTombstoneList {
        self.frozen_tombstones.get_or_init(|| {
            let shared: Vec<RawTombstone> = self.with_range_tombstones(|ranges| {
                ranges
                    .iter()
                    .map(|rt| (Arc::clone(&rt.begin), Arc::clone(&rt.end), rt.seq))
                    .collect()
            });
            FragmentedRangeTombstoneList::from_shared(&shared)
        })
    }

    /// Find the highest-seq range tombstone covering `user_key` with seq <= `read_seq`.
    /// Returns 0 if no covering tombstone exists.
    /// Lock-free while active (via tiered interval index) and once frozen
    /// (via `FragmentedRangeTombstoneList`).
    pub fn max_covering_tombstone_seq(
        &self,
        user_key: &[u8],
        read_seq: SequenceNumber,
    ) -> SequenceNumber {
        if !self.has_range_deletions() {
            return 0;
        }
        if self.immutable.load(Ordering::Acquire) {
            let frozen = self.init_frozen_tombstones();
            if self.active_tombstones.load().is_some() {
                self.active_tombstones.store(None);
                self.tombstone_index_bytes
                    .store(frozen.approximate_heap_bytes(), Ordering::Relaxed);
            }
            return frozen.max_covering_tombstone_seq(user_key, read_seq);
        }
        if let Some(idx) = self.active_tombstones.load().as_ref() {
            return idx.max_covering_tombstone_seq(user_key, read_seq);
        }
        if self.immutable.load(Ordering::Acquire) {
            return self
                .init_frozen_tombstones()
                .max_covering_tombstone_seq(user_key, read_seq);
        }
        0
    }

    /// Borrow the list for the duration of a read, so callers can select endpoints before copying them.
    pub(crate) fn with_range_tombstones<R>(
        &self,
        read: impl FnOnce(&[MemRangeTombstone]) -> R,
    ) -> R {
        if !self.has_range_deletions() {
            return read(&[]);
        }
        read(&self.range_tombstones.read())
    }

    /// Return all range tombstones as (begin, end, seq) triples.
    #[cfg(test)]
    pub fn get_range_tombstones(&self) -> Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> {
        self.with_range_tombstones(|ranges| {
            ranges
                .iter()
                .map(|rt| (rt.begin.to_vec(), rt.end.to_vec(), rt.seq))
                .collect()
        })
    }

    /// Return true if empty.
    pub fn is_empty(&self) -> bool {
        self.approximate_size.load(Ordering::Relaxed) == 0
    }

    /// Return `true` if any point entry or range tombstone in this memtable
    /// overlaps the closed user-key span `[min_key, max_key]`.
    pub(crate) fn overlaps_user_key_range(&self, min_key: &[u8], max_key: &[u8]) -> bool {
        if self.is_empty() {
            return false;
        }
        let tag = (!crate::types::pack_sequence_and_type(
            crate::types::MAX_SEQUENCE_NUMBER,
            ValueType::RangeDeletion,
        ))
        .to_be_bytes();
        if let Some((encoded, _)) = self.inner.lower_bound(min_key, &tag)
            && crate::types::user_key(encoded) <= max_key
        {
            return true;
        }
        if self.has_range_deletions() {
            if let Some(idx) = self.active_tombstones.load().as_ref() {
                return idx.overlaps_user_key_range(min_key, max_key);
            }
            let tombstones = self.range_tombstones.read();
            if tombstones
                .iter()
                .any(|rt| rt.begin.as_ref() <= max_key && rt.end.as_ref() > min_key)
            {
                return true;
            }
        }
        false
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedicated_node_preserves_room_in_active_slab() {
        let mt = MemTable::new();
        mt.put(b"a", b"", 1, ValueType::Value);
        let initial = mt.approximate_size();
        assert_eq!(initial, 4096);
        let value = vec![0xab; (1 << 18) + 1];
        mt.put(b"big", &value, 2, ValueType::Value);
        let after_large = mt.approximate_size();
        assert!((value.len()..value.len() + 256).contains(&(after_large - initial)));
        mt.put(b"c", b"x", 3, ValueType::Value);
        assert_eq!(mt.approximate_size(), after_large);
        assert_eq!(mt.get(b"big", 2), Some(Some(value)));
    }

    #[test]
    fn slab_density_for_small_keys_and_values() {
        let mt = MemTable::new();
        let count = 120_000u128;
        for i in 0..count {
            mt.put(&i.to_be_bytes(), &[0; 16], i as u64, ValueType::Value);
        }
        let bytes = mt.approximate_size();
        // The accounting includes slab tail space and 8-byte alignment.
        assert!(
            bytes / count as usize <= 82,
            "{bytes} allocated for {count} entries"
        );
        eprintln!("{bytes} slab bytes for {count} distinct 16B key/value pairs");
    }

    /// Frozen memtables answer coverage checks from a fragmented index; it
    /// must agree with the linear scan used while the memtable is active.
    #[test]
    fn frozen_tombstone_index_matches_linear_scan() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = |bound: u64| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % bound
        };
        let key = |n: u64| format!("k{n:03}").into_bytes();
        let active = MemTable::new();
        let frozen = MemTable::new();
        for seq in 1..=200 {
            let begin = next(100);
            let end = begin + 1 + next(20);
            for mem in [&active, &frozen] {
                mem.put(&key(begin), &key(end), seq, ValueType::RangeDeletion);
            }
        }
        frozen.mark_immutable();
        for _ in 0..2000 {
            let (k, snapshot) = (key(next(130)), next(220));
            assert_eq!(
                frozen.max_covering_tombstone_seq(&k, snapshot),
                active.max_covering_tombstone_seq(&k, snapshot),
                "key {:?} at snapshot {snapshot}",
                String::from_utf8_lossy(&k)
            );
        }
    }

    /// Regression: every coverage check took one `Mutex` and scanned all
    /// tombstones, so concurrent gets serialized. Frozen memtables now take
    /// no lock, and active-memtable readers share it.
    #[test]
    fn tombstone_checks_do_not_serialize_readers() {
        use std::sync::mpsc;
        use std::time::Duration;

        let mem = MemTable::new();
        mem.put(b"a", b"m", 5, ValueType::RangeDeletion);
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let reader = mem.range_tombstones.read();
            scope.spawn(|| {
                done_tx
                    .send(mem.max_covering_tombstone_seq(b"c", 10))
                    .unwrap()
            });
            assert_eq!(done_rx.recv_timeout(Duration::from_secs(10)), Ok(5));
            drop(reader);
        });

        mem.mark_immutable();
        // The first check after freeze builds the index; later ones are lock-free.
        assert_eq!(mem.max_covering_tombstone_seq(b"c", 10), 5);
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let writer = mem.range_tombstones.write();
            scope.spawn(|| {
                done_tx
                    .send(mem.max_covering_tombstone_seq(b"c", 10))
                    .unwrap()
            });
            assert_eq!(done_rx.recv_timeout(Duration::from_secs(10)), Ok(5));
            drop(writer);
        });
    }

    /// Regression: a lookup for a key the memtable does not hold cloned the
    /// next entry's key and value before comparing user keys.
    #[test]
    fn get_miss_does_not_copy_the_neighbouring_entry() {
        let mem = MemTable::new();
        mem.put(b"b", &vec![7u8; 1 << 20], 1, ValueType::Value);
        let (result, bytes) = crate::test_alloc::count_allocated(|| mem.get_with_seq(b"a", 10));
        assert!(result.is_none());
        assert!(bytes < 4096, "a memtable miss allocated {bytes} bytes");
    }

    #[test]
    fn test_memtable_put_get() {
        let mt = MemTable::new();
        mt.put(b"key1", b"value1", 1, ValueType::Value);
        mt.put(b"key2", b"value2", 2, ValueType::Value);

        // Should find key1 at seq >= 1
        assert_eq!(mt.get(b"key1", 1), Some(Some(b"value1".to_vec())));
        assert_eq!(mt.get(b"key2", 2), Some(Some(b"value2".to_vec())));

        // key1 not visible at seq 0
        assert_eq!(mt.get(b"key1", 0), None);
    }

    #[test]
    fn test_memtable_delete() {
        let mt = MemTable::new();
        mt.put(b"key1", b"value1", 1, ValueType::Value);
        mt.put(b"key1", b"", 2, ValueType::Deletion);

        // At seq 2, should see tombstone
        assert_eq!(mt.get(b"key1", 2), Some(None));
        // At seq 1, should see the value
        assert_eq!(mt.get(b"key1", 1), Some(Some(b"value1".to_vec())));
    }

    #[test]
    fn test_memtable_overwrite() {
        let mt = MemTable::new();
        mt.put(b"key1", b"v1", 1, ValueType::Value);
        mt.put(b"key1", b"v2", 2, ValueType::Value);

        assert_eq!(mt.get(b"key1", 2), Some(Some(b"v2".to_vec())));
        assert_eq!(mt.get(b"key1", 1), Some(Some(b"v1".to_vec())));
        assert_eq!(mt.get(b"key1", 10), Some(Some(b"v2".to_vec())));
    }

    #[test]
    fn test_memtable_iterator() {
        let mt = MemTable::new();
        mt.put(b"b", b"2", 1, ValueType::Value);
        mt.put(b"a", b"1", 2, ValueType::Value);
        mt.put(b"c", b"3", 3, ValueType::Value);

        let entries: Vec<_> = mt.iter().collect();
        // Should be sorted by internal key ordering: a, b, c
        assert_eq!(entries.len(), 3);
        let user_keys: Vec<Vec<u8>> = entries
            .iter()
            .map(|(k, _)| {
                let ik = InternalKey::from_encoded(k.clone());
                ik.user_key().to_vec()
            })
            .collect();
        assert_eq!(user_keys[0], b"a");
        assert_eq!(user_keys[1], b"b");
        assert_eq!(user_keys[2], b"c");
    }

    #[test]
    fn test_memtable_approximate_size() {
        let mt = MemTable::new();
        assert_eq!(mt.approximate_size(), 0);
        mt.put(b"key", b"value", 1, ValueType::Value);
        assert!(mt.approximate_size() > 0);
    }

    #[test]
    fn test_memtable_single_writer_concurrent_readers() {
        use std::sync::Arc;
        use std::thread;

        let mt = Arc::new(MemTable::new());
        let num_entries = 800;

        // Single writer inserts all entries first (matches DB group commit model).
        for i in 0..num_entries {
            let t = i / 100;
            let j = i % 100;
            let key = format!("t{}_k{:04}", t, j);
            let val = format!("t{}_v{}", t, j);
            let seq = (i + 1) as u64;
            mt.put(key.as_bytes(), val.as_bytes(), seq, ValueType::Value);
        }

        // Spawn concurrent readers to verify all entries.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let mt = Arc::clone(&mt);
            handles.push(thread::spawn(move || {
                for i in 0..num_entries {
                    let t = i / 100;
                    let j = i % 100;
                    let key = format!("t{}_k{:04}", t, j);
                    let val = format!("t{}_v{}", t, j);
                    let seq = (i + 1) as u64;
                    let result = mt.get(key.as_bytes(), seq);
                    assert_eq!(
                        result,
                        Some(Some(val.into_bytes())),
                        "missing key {} at seq {}",
                        key,
                        seq
                    );
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify total count via iterator
        let total: usize = mt.iter().count();
        assert_eq!(total, num_entries);
    }

    #[test]
    fn test_memtable_prefix_similar_keys() {
        let mt = MemTable::new();
        mt.put(b"a", b"val_a", 1, ValueType::Value);
        mt.put(b"ab", b"val_ab", 2, ValueType::Value);
        mt.put(b"abc", b"val_abc", 3, ValueType::Value);

        // Each key should be independently retrievable
        assert_eq!(mt.get(b"a", 10), Some(Some(b"val_a".to_vec())));
        assert_eq!(mt.get(b"ab", 10), Some(Some(b"val_ab".to_vec())));
        assert_eq!(mt.get(b"abc", 10), Some(Some(b"val_abc".to_vec())));

        // Non-existent prefix-similar keys should not be found
        assert_eq!(mt.get(b"abcd", 10), None);
        assert_eq!(mt.get(b"b", 10), None);

        // Visibility by sequence: "abc" at seq 3 should not be visible at seq 2
        assert_eq!(mt.get(b"abc", 2), None);
        assert_eq!(mt.get(b"abc", 3), Some(Some(b"val_abc".to_vec())));

        // Iterator should yield keys in sorted order: a, ab, abc
        let entries: Vec<_> = mt.iter().collect();
        assert_eq!(entries.len(), 3);
        let user_keys: Vec<Vec<u8>> = entries
            .iter()
            .map(|(k, _)| InternalKey::from_encoded(k.clone()).user_key().to_vec())
            .collect();
        assert_eq!(user_keys[0], b"a");
        assert_eq!(user_keys[1], b"ab");
        assert_eq!(user_keys[2], b"abc");
    }

    #[test]
    fn test_memtable_is_empty_states() {
        let mt = MemTable::new();

        // Brand new memtable should be empty
        assert!(mt.is_empty());
        assert_eq!(mt.approximate_size(), 0);

        // After a single put, no longer empty
        mt.put(b"k", b"v", 1, ValueType::Value);
        assert!(!mt.is_empty());
        assert!(mt.approximate_size() > 0);

        // After a deletion, still not empty (tombstone is an entry)
        let mt2 = MemTable::new();
        assert!(mt2.is_empty());
        mt2.put(b"k", b"", 1, ValueType::Deletion);
        assert!(!mt2.is_empty());
    }

    #[test]
    fn get_with_seq_at_range_tombstone_sequence_does_not_skip_to_older_value() {
        let mt = MemTable::new();
        mt.put(b"k", b"old_val", 5, ValueType::Value);
        mt.put(b"k", b"z", 10, ValueType::RangeDeletion);

        assert_eq!(
            mt.get_with_seq(b"k", 9),
            Some((Some(b"old_val".to_vec()), 5))
        );
        assert_eq!(mt.get_with_seq(b"k", 10), Some((None, 10)));
        assert_eq!(mt.get_with_seq(b"k", 11), Some((None, 10)));
    }

    #[test]
    fn active_tiered_tombstone_index_matches_linear_scan_across_tiers_and_freeze() {
        let mut state = 0x243f_6a88_85a3_08d3u64;
        let mut next = |bound: u64| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % bound
        };
        let key = |n: u64| format!("rk_{n:04}").into_bytes();

        let mem = MemTable::new();
        let mut truth: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> = Vec::new();
        let mut seq = 1u64;

        // Mix single `put` and multi-entry `put_batch` across > 150 range deletions
        // (spanning multiple Bentley-Saxe tiers + non-empty tail, plus ignored empty ranges).
        for round in 0..40u64 {
            if round % 3 == 0 {
                let batch_first = seq;
                let mut batch = crate::types::WriteBatch::new();
                for _ in 0..4 {
                    let b = next(120);
                    let e = if next(10) == 0 {
                        b.saturating_sub(1) // empty/inverted range -> ignored by WriteBatch
                    } else {
                        b + 1 + next(35)
                    };
                    let bk = key(b);
                    let ek = key(e);
                    let before_len = batch.len();
                    batch.delete_range(&bk, &ek);
                    if batch.len() > before_len {
                        truth.push((bk, ek, seq));
                        seq += 1;
                    }
                }
                mem.put_batch(&batch, batch_first);
            } else {
                let b = next(120);
                let e = if next(12) == 0 {
                    b.saturating_sub(1) // empty/inverted range -> ignored by MemTable::put
                } else {
                    b + 1 + next(35)
                };
                let bk = key(b);
                let ek = key(e);
                mem.put(&bk, &ek, seq, ValueType::RangeDeletion);
                if bk < ek {
                    truth.push((bk, ek, seq));
                    seq += 1;
                }
            }
        }
        assert!(truth.len() >= 70);

        let linear_max = |uk: &[u8], read_seq: SequenceNumber| -> SequenceNumber {
            truth
                .iter()
                .filter(|(b, e, s)| *s <= read_seq && uk >= b.as_slice() && uk < e.as_slice())
                .map(|(_, _, s)| *s)
                .max()
                .unwrap_or(0)
        };

        // 1. Active tiered index parity + range-overlap parity
        for _ in 0..3_000 {
            let uk = key(next(170));
            let snap = next(seq + 20);
            assert_eq!(
                mem.max_covering_tombstone_seq(&uk, snap),
                linear_max(&uk, snap),
                "active mismatch for {:?} at snap {snap}",
                String::from_utf8_lossy(&uk)
            );
        }
        for _ in 0..500 {
            let a = next(170);
            let b = a + next(20);
            let (lo, hi) = (key(a), key(b));
            let expected = truth
                .iter()
                .any(|(rb, re, _)| rb.as_slice() <= hi.as_slice() && re.as_slice() > lo.as_slice());
            assert_eq!(mem.overlaps_user_key_range(&lo, &hi), expected);
        }

        // 2. Freeze transition parity & active index release
        mem.mark_immutable();
        for _ in 0..1_000 {
            let uk = key(next(170));
            let snap = next(seq + 20);
            assert_eq!(
                mem.max_covering_tombstone_seq(&uk, snap),
                linear_max(&uk, snap)
            );
        }
        assert!(mem.active_tombstones.load().is_none());
    }

    #[test]
    fn nested_small_range_deletion_shares_large_endpoints_and_accounts_index_bytes() {
        let mem = MemTable::new();
        let big_begin = vec![b'a'; 1 << 20];
        let big_end = vec![b'z'; 1 << 20];
        mem.put(&big_begin, &big_end, 1, ValueType::RangeDeletion);
        let after_first = mem.approximate_size();
        // Arena dedicated slab (~2 MiB) + shared endpoint Arcs (~2 MiB) + index overhead
        assert!((4 << 20..5 << 20).contains(&after_first));

        // Inserting a tiny nested `[m, n)` range must NOT clone the 1 MiB endpoints!
        let ((), second_alloc) = crate::test_alloc::count_allocated(|| {
            mem.put(b"m", b"n", 2, ValueType::RangeDeletion);
        });
        assert!(
            second_alloc < 8 * 1024,
            "tiny nested range deletion allocated {second_alloc} bytes"
        );
        assert_eq!(mem.max_covering_tombstone_seq(b"b", 1), 1);
        assert_eq!(mem.max_covering_tombstone_seq(b"m0", 1), 1);
        assert_eq!(mem.max_covering_tombstone_seq(b"m0", 2), 2);

        // Even across a tier merge (8 range deletions), endpoint bytes remain shared!
        let ((), merge_alloc) = crate::test_alloc::count_allocated(|| {
            for seq in 3..=8 {
                mem.put(b"m1", b"m9", seq, ValueType::RangeDeletion);
            }
        });
        assert!(
            merge_alloc < 32 * 1024,
            "tier merge copied large endpoints: {merge_alloc} bytes"
        );
        assert!(mem.tombstone_index_bytes.load(Ordering::Relaxed) > 0);
        assert_eq!(mem.get_range_tombstones().len(), 8);
    }
}
