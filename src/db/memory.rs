use super::DB;
use crate::BlockCachePool;

/// Current memtable estimates and member-local L0 cache pins for one DB.
///
/// The memtable gauges sample the membership of a single read view. Its
/// active table can grow concurrently. Retired generations held by readers
/// and blocks retained solely by callers of [`DB::get_pinned`] are outside
/// these gauges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DbMemoryUsage {
    /// Estimated active memtable bytes, including allocated arena slabs and
    /// range-deletion storage.
    pub active_memtable_bytes: usize,
    /// Number of immutable memtables visible in the sampled view.
    pub immutable_memtable_count: usize,
    /// Sum of the estimated sizes in bytes of that view's immutable memtables.
    pub immutable_memtable_bytes: usize,
    /// Payload bytes of blocks pinned in this member's L0 pin map.
    pub l0_pin_bytes: u64,
}

impl DB {
    /// Sample this handle's current memtables and its own L0 block pins.
    /// This call acquires the published read view without taking the writer
    /// mutex. The pin map is sampled independently. After close completes,
    /// the handle's gauges are zero, including when a caller retains bytes
    /// obtained from an older view.
    pub fn memory_usage(&self) -> DbMemoryUsage {
        let view = self.read.get_super_version();
        DbMemoryUsage {
            active_memtable_bytes: view.active_memtable.approximate_size(),
            immutable_memtable_count: view.immutable_memtables.len(),
            immutable_memtable_bytes: view
                .immutable_memtables
                .iter()
                .map(|table| table.approximate_size())
                .sum(),
            l0_pin_bytes: self.block_cache.pinned_bytes(),
        }
    }

    /// Access the private or shared pool used by this handle. Read
    /// [`BlockCachePool::usage`] here even when the pool is private.
    pub fn block_cache_pool(&self) -> &BlockCachePool {
        &self.block_cache.pool
    }
}
