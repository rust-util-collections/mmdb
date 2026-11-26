//! Core DB implementation with WAL, MemTable, SST, MANIFEST, and Iterator.

mod directory;
mod flush;
mod ingest;
mod memory;
mod read;
mod snapshot;
use flush::{FlushContext, FlushQueue};
pub use memory::DbMemoryUsage;
use read::ReadState;
use snapshot::NEXT_DB_ID;
pub use snapshot::{ArcSnapshot, Snapshot, SnapshotRef};

use std::{
    collections::{HashSet, VecDeque},
    fs::{self, OpenOptions},
    io, mem,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use arc_swap::ArcSwap;
use parking_lot::{Condvar, Mutex, MutexGuard, RwLock};

use crate::cache::block_cache::BlockCache;
use crate::cache::table_cache::TableCache;
use crate::compaction::LeveledCompaction;
use crate::compaction::leveled::{CompactionContext, CompactionHint};
use crate::error::{Error, Result, ResultExt};
use crate::iterator::db_iter::DBIterator;
use crate::iterator::level_iter::LevelIterator;
use crate::iterator::merge::IterSource;
use crate::manifest::file_numbers::{FileNumbers, PendingSsts};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::manifest::version_set::{VersionSet, confirm_manifest_durable, sync_new_sst_parent_dir};
use crate::memtable::MemTable;
use crate::memtable::skiplist::MemTableCursorIter;
use crate::options::{
    CompactionFilter, CompactionFilterDecision, DbOptions, DurabilityMode, ReadOptions,
    WriteOptions,
};
use crate::rate_limiter::RateLimiter;
use crate::sst::SstBuilds;
use crate::sst::filter::BloomFilter;
use crate::sst::table_builder::{
    MAX_RANGE_STRIPES, META_BLOCK_SPLIT_THRESHOLD, TableBuildOptions, TableBuildResult,
    TableBuilder,
};
use crate::sst::table_reader::TableIterator;
use crate::stats::DbStats;
use crate::types::{
    self, MAX_RANGE_DELETE_SIZE, MAX_SEQUENCE_NUMBER, MAX_USER_KEY_SIZE, MAX_WRITE_ENTRY_SIZE,
    SequenceNumber, ValueType, WriteBatch, WriteBatchWithIndex, tombstone_overlaps_bounds,
};
use crate::wal::{WalReader, WalWriter};
use crate::{PinnedSlice, options::IterConfig};

/// Maximum number of dead-key registrations probed for auto-pruning after
/// each flush. Bounds the extra read work per pass; the probes run off the
/// writer-serializing paths (explicit `flush()` prunes after releasing the
/// write-queue lock; an auto-flush leader prunes after waking its waiters).
/// Explicit `compact()` / `compact_range()` calls prune without a cap.
const DEAD_KEY_PRUNE_PER_FLUSH: usize = 1024;

/// How many settled dead keys `prune_settled_dead_keys` re-verifies per
/// `write_queue` acquisition. Only each key's own re-verify-and-remove pair
/// needs to be atomic against concurrent writes, so the locked phase is
/// chunked: writers wait at most one chunk of lookups instead of the whole
/// (uncapped, for explicit `compact`/`compact_range`) settled list.
const DEAD_KEY_RECONFIRM_CHUNK: usize = 64;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum DeadKeySweepState {
    Idle,
    Pending,
    Running,
    RunningPending,
}

impl DeadKeySweepState {
    fn from_raw(state: u8) -> Self {
        match state {
            value if value == Self::Idle as u8 => Self::Idle,
            value if value == Self::Pending as u8 => Self::Pending,
            value if value == Self::Running as u8 => Self::Running,
            value if value == Self::RunningPending as u8 => Self::RunningPending,
            _ => unreachable!("invalid dead-key sweep state"),
        }
    }
}

/// Coalesces sweep requests without losing work queued during a running pass.
struct DeadKeySweepScheduler {
    state: AtomicU8,
    /// Per-DB seams for scheduling real workers without affecting other tests.
    #[cfg(test)]
    before_deferred_finish: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    completed_passes: AtomicUsize,
}

impl DeadKeySweepScheduler {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(DeadKeySweepState::Idle as u8),
            #[cfg(test)]
            before_deferred_finish: Mutex::new(None),
            #[cfg(test)]
            completed_passes: AtomicUsize::new(0),
        }
    }

    fn queue(&self) {
        let _ = self
            .state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                match DeadKeySweepState::from_raw(current) {
                    DeadKeySweepState::Idle => Some(DeadKeySweepState::Pending as u8),
                    DeadKeySweepState::Running => Some(DeadKeySweepState::RunningPending as u8),
                    DeadKeySweepState::Pending | DeadKeySweepState::RunningPending => None,
                }
            });
    }

    fn is_pending(&self) -> bool {
        DeadKeySweepState::from_raw(self.state.load(Ordering::Acquire))
            == DeadKeySweepState::Pending
    }

    fn try_start(&self) -> bool {
        self.state
            .compare_exchange(
                DeadKeySweepState::Pending as u8,
                DeadKeySweepState::Running as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn retry_after_snapshot_release(&self) -> bool {
        match self
            .state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                match DeadKeySweepState::from_raw(current) {
                    DeadKeySweepState::Running => Some(DeadKeySweepState::RunningPending as u8),
                    DeadKeySweepState::Idle
                    | DeadKeySweepState::Pending
                    | DeadKeySweepState::RunningPending => None,
                }
            }) {
            Ok(_) => true,
            Err(current) => matches!(
                DeadKeySweepState::from_raw(current),
                DeadKeySweepState::Pending | DeadKeySweepState::RunningPending
            ),
        }
    }

    fn finish(&self, retry: bool) {
        let _ = self
            .state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                match DeadKeySweepState::from_raw(current) {
                    DeadKeySweepState::Running if retry => Some(DeadKeySweepState::Pending as u8),
                    DeadKeySweepState::Running => Some(DeadKeySweepState::Idle as u8),
                    DeadKeySweepState::RunningPending => Some(DeadKeySweepState::Pending as u8),
                    DeadKeySweepState::Idle | DeadKeySweepState::Pending => None,
                }
            });
    }
}

/// Tracks active snapshots so compaction doesn't delete data still needed by readers.
struct SnapshotList {
    snapshots: Mutex<Vec<SequenceNumber>>,
    count: AtomicUsize,
    #[cfg(test)]
    range_stripe_limit: AtomicUsize,
}

impl SnapshotList {
    fn new() -> Self {
        Self {
            snapshots: Mutex::new(Vec::new()),
            count: AtomicUsize::new(0),
            #[cfg(test)]
            range_stripe_limit: AtomicUsize::new(MAX_RANGE_STRIPES),
        }
    }

    fn acquire(&self, seq: SequenceNumber) -> SequenceNumber {
        let mut snapshots = self.snapshots.lock();
        snapshots.push(seq);
        self.count.store(snapshots.len(), Ordering::Relaxed);
        seq
    }

    fn release(&self, seq: SequenceNumber) {
        let mut v = self.snapshots.lock();
        if let Some(pos) = v.iter().position(|&s| s == seq) {
            v.swap_remove(pos);
            self.count.store(v.len(), Ordering::Relaxed);
        }
    }

    fn as_sorted_vec(&self) -> Vec<SequenceNumber> {
        let mut v = self.snapshots.lock().clone();
        v.sort_unstable();
        v.dedup();
        v
    }

    fn is_empty(&self) -> bool {
        self.snapshots.lock().is_empty()
    }
}

/// Lifecycle bitflags of a queued [`WriteRequest`].
const REQ_QUEUED: u8 = 0;
const STATE_PARKED: u8 = 1 << 0;
const STATE_IN_GROUP: u8 = 1 << 1;
const STATE_WAKE: u8 = 1 << 2;
const STATE_DONE: u8 = 1 << 3;

type WriteGroup = smallvec::SmallVec<[*mut WriteRequest; 16]>;

/// A pending write request for group commit.
struct WriteRequest {
    batch: WriteBatch,
    kind: WriteRequestKind,
    no_slowdown: bool,
    durability: DurabilityMode,
    waiter: Option<thread::Thread>,
    result: Option<Result<()>>,
    state: AtomicU8,
}

#[derive(Clone, Copy)]
enum WriteRequestKind {
    Batch,
    WalFence { target_seq: u64 },
}

/// Group commit queue state. The `leader_active` flag prevents new arrivals
/// from becoming false "leaders" while the real leader holds `inner` lock.
struct WriteQueueState {
    waiting_barriers: usize,
    queue: VecDeque<*mut WriteRequest>,
    leader_active: bool,
}

// SAFETY: each pointer refers to a queued writer's live stack frame. A follower
// stays in `enqueue_write` projecting only `(*req_ptr).state` until the leader
// swaps `STATE_DONE` with `AcqRel` (after writing `result` and taking `waiter`).
// Neither side forms `&WriteRequest` or `&mut WriteRequest` while queued,
// preserving Stacked/Tree Borrows provenance, and no thread dereferences a
// request pointer after `STATE_DONE` is published.
unsafe impl Send for WriteQueueState {}

impl WriteQueueState {
    fn can_drain_with(&self, new_payload_len: usize, write_buffer_size: usize) -> bool {
        if self.queue.is_empty() {
            return true;
        }
        const MAX_WRITE_BATCH_GROUP_BYTES: usize = 16 << 20;
        let cap = (write_buffer_size / 2).min(MAX_WRITE_BATCH_GROUP_BYTES);
        let mut bytes = new_payload_len;
        if bytes > cap {
            return false;
        }
        for &request in &self.queue {
            // SAFETY: queued requests remain live until drained and completed.
            let len = unsafe { (*request).batch.payload.len() };
            bytes = bytes.saturating_add(len);
            if bytes > cap {
                return false;
            }
        }
        true
    }

    fn drain_group(&mut self, write_buffer_size: usize) -> WriteGroup {
        const MAX_WRITE_BATCH_GROUP_BYTES: usize = 16 << 20;
        let cap = (write_buffer_size / 2).min(MAX_WRITE_BATCH_GROUP_BYTES);
        let mut bytes = 0usize;
        #[cfg(feature = "bench-metrics")]
        let mut group_bytes = 0;
        let count = self
            .queue
            .iter()
            .enumerate()
            .take_while(|&(i, &request)| {
                // SAFETY: the owner's stack frame remains live and its batch is
                // immutable until the leader completes the request.
                let batch = unsafe { &(*request).batch };
                bytes = bytes.saturating_add(batch.payload.len());
                // A single batch is atomic and may exceed the group limit.
                let admitted = i == 0 || bytes <= cap;
                #[cfg(feature = "bench-metrics")]
                if admitted {
                    group_bytes = bytes as u64;
                }
                admitted
            })
            .count();
        #[cfg(feature = "bench-metrics")]
        crate::profile::record(|p| {
            p.groups += 1;
            p.requests += count as u64;
            p.payload_bytes += group_bytes;
            p.max_group_requests = p.max_group_requests.max(count as u64);
            p.max_group_bytes = p.max_group_bytes.max(group_bytes);
        });
        let group: WriteGroup = self.queue.drain(..count).collect();
        for &rp in &group {
            // SAFETY: drained requests remain live until `complete_write_group`
            // swaps `STATE_DONE`.
            unsafe {
                (*rp).state.fetch_or(STATE_IN_GROUP, Ordering::AcqRel);
            }
        }
        group
    }
}

/// RAII guard holding `write_queue` after draining any in-flight write leader.
/// Unless converted into write leadership via [`Self::claim_leadership`],
/// dropping the guard wakes the head queued writer when no leader or other
/// barrier remains active.
pub(super) struct WriteBarrier<'a> {
    db: &'a DB,
    wq: Option<MutexGuard<'a, WriteQueueState>>,
}

impl WriteBarrier<'_> {
    pub(super) fn claim_leadership(&mut self) {
        if let Some(ref mut wq) = self.wq {
            wq.leader_active = true;
        }
    }
}

impl Drop for WriteBarrier<'_> {
    fn drop(&mut self) {
        let _ = self.db;
        if let Some(wq) = self.wq.take() {
            let wake = if !wq.leader_active && wq.waiting_barriers == 0 {
                DB::wake_queue_head(&wq)
            } else {
                None
            };
            drop(wq);
            if let Some(t) = wake {
                t.unpark();
            }
        }
    }
}

/// State captured during the freeze phase of a flush.
/// Holds the frozen memtable and metadata needed to complete the flush
/// after releasing the lock.
struct FrozenMemtable {
    old_mem: Arc<MemTable>,
    file_numbers: Arc<FileNumbers>,
    old_wal_number: u64,
    new_wal_number: u64,
    last_sequence: u64,
    snapshots: Vec<u64>,
}

/// Atomically install a fresh SuperVersion into the given `ArcSwap`.
/// Shared between `DB::install_super_version` and compaction threads
/// (which only hold an `Arc<ArcSwap<…>>`, not `&DB`).
fn refresh_super_version(target: &ArcSwap<SuperVersion>, inner: &DBInner) {
    target.store(Arc::new(SuperVersion {
        active_memtable: inner.active_memtable.clone(),
        immutable_memtables: inner.immutable_memtables.iter().cloned().collect(),
        version: inner.versions.current(),
        ingest_ceiling: inner.ingest_ceiling.clone(),
    }));
}

/// Table options for memtable flush outputs, which always land in L0.
/// Shared by live flushes and the open-time recovery flush.
fn l0_build_opts(options: &DbOptions) -> TableBuildOptions {
    let compression = if !options.compression_per_level.is_empty() {
        options.compression_per_level[0]
    } else {
        options.compression
    };
    TableBuildOptions {
        block_size: options.block_size,
        block_restart_interval: options.block_restart_interval,
        bloom_bits_per_key: options.bloom_bits_for_level(0),
        prefix_bloom_bits_per_key: Some(options.bloom_bits_per_key),
        compression,
        prefix_len: options.prefix_len,
        block_property_collectors: options
            .block_property_collectors
            .iter()
            .map(|f| f())
            .collect(),
    }
}

/// State shared by explicit full-level rewrites and the background sweep.
struct LevelRewrite<'a> {
    inner: &'a Mutex<DBInner>,
    snapshots: &'a SnapshotList,
    claims: &'a Arc<CompactionClaims>,
    table_cache: &'a Arc<TableCache>,
    block_cache: &'a Arc<BlockCache>,
    super_version: &'a ArcSwap<SuperVersion>,
    l0_count: &'a AtomicUsize,
}

impl LevelRewrite<'_> {
    fn run(&self, level: usize, ctx: &CompactionContext<'_>, mut move_l0: bool) -> Result<bool> {
        let _build = ctx.builds.enter();
        loop {
            let (task, numbers, snapshots, bottom, epoch, claim) = {
                let inner = self.inner.lock();
                let version = inner.versions.current();
                if level == 0 && !inner.immutable_memtables.is_empty() && !move_l0 {
                    return Ok(false);
                }
                let mut claimed = self.claims.lock();
                let task = if level == 0 && move_l0 {
                    LeveledCompaction::pick_l0_compaction(&version, &claimed)
                } else {
                    LeveledCompaction::full_level_task(&version, level)
                };
                let Some(task) = task else {
                    if move_l0 && level == 0 && version.l0_file_count() != 0 {
                        drop(claimed);
                        drop(inner);
                        self.claims.wait_until_empty();
                        continue;
                    }
                    return Ok(level != 0 || inner.immutable_memtables.is_empty());
                };
                let inputs: Vec<_> = task
                    .input_files_level
                    .iter()
                    .chain(&task.input_files_next)
                    .cloned()
                    .collect();
                let selected: Vec<_> = inputs.iter().map(|tf| tf.meta.number).collect();
                if selected.iter().any(|n| claimed.contains(n)) {
                    drop(claimed);
                    drop(inner);
                    self.claims.wait_until_empty();
                    continue;
                }
                let snapshots = self.snapshots.as_sorted_vec();
                let bottom = LeveledCompaction::is_bottommost_level(
                    &version,
                    level,
                    ctx.options.num_levels,
                    &inputs,
                );
                claimed.extend(selected.iter().copied());
                let claim = CompactionClaim {
                    claims: self.claims.clone(),
                    numbers: selected,
                };
                (
                    task,
                    inner.versions.file_numbers.clone(),
                    snapshots,
                    bottom,
                    inner.l0_install_epoch,
                    claim,
                )
            };
            if task.target_level == level
                && task.input_files_level.len() == 1
                && (!snapshots.is_empty()
                    || ctx
                        .options
                        .compaction_filter
                        .as_ref()
                        .is_none_or(|f| f.is_noop()))
            {
                return Ok(true);
            }
            let ctx = CompactionContext {
                active_snapshots: &snapshots,
                ..*ctx
            };
            let output = LeveledCompaction::execute_compaction_io(&ctx, &task, &numbers, bottom)?;
            self.table_cache
                .prewarm(output.edit.new_files.iter().map(|(_, m)| m.number));
            let (cleanup, manifest) = {
                let mut inner = self.inner.lock();
                if level == 0 && task.target_level == 0 && epoch != inner.l0_install_epoch {
                    drop(inner);
                    // A flush can allocate an older file number than our output.
                    // Discard the rewrite, then repick a move into L1 where file
                    // numbers do not determine read order.
                    for (_, m) in output.edit.new_files {
                        self.table_cache.evict(m.number);
                        let _ = fs::remove_file(ctx.db_path.join(format!("{:06}.sst", m.number)));
                    }
                    drop(claim);
                    move_l0 = true;
                    continue;
                }
                let cleanup = LeveledCompaction::install_compaction(
                    output,
                    &mut inner.versions,
                    Some(self.table_cache),
                    Some(self.block_cache),
                    ctx.db_path,
                    ctx.stats,
                    !self.snapshots.is_empty(),
                )?;
                if cleanup.installed && level == 0 {
                    for tf in &task.input_files_level {
                        self.block_cache.unpin_file(tf.meta.number);
                    }
                }
                self.l0_count
                    .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
                refresh_super_version(self.super_version, &inner);
                (cleanup, inner.versions.manifest_sync_handle())
            };
            if !cleanup.installed {
                continue;
            }
            confirm_manifest_durable(&manifest, ctx.manifest_poisoned)?;
            LeveledCompaction::run_post_compaction_cleanup(&cleanup, ctx.db_path);
            // Retry the sweep after the outstanding generation publishes its
            // L0 files. This move may also have introduced work at L1.
            return Ok(!move_l0);
        }
    }
}

/// The `LOCK` file holding the directory flock. Dropping it calls `LOCK_UN`
/// before the fd closes: flock belongs to the open file description, and a
/// plain close releases it only once every reference to that description is
/// gone (a deferred final `fput`, or a descriptor shared with a child), so a
/// same-process reopen could otherwise fail to lock. Every holder uses this —
/// the open path's error returns as well as the constructed `DB`.
struct DirLock(fs::File);

impl Drop for DirLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: `self.0` is open for the whole call and is the fd that
            // acquired the flock.
            let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

struct DeferredTeardown {
    flush_queue: Arc<FlushQueue>,
    storage_path: PathBuf,
    block_cache: Arc<BlockCache>,
    table_cache: Arc<TableCache>,
    inner: Arc<Mutex<DBInner>>,
    super_version: Arc<ArcSwap<SuperVersion>>,
    dir_sync: Arc<Mutex<Option<fs::File>>>,
    lock_file: Option<DirLock>,
}

impl DeferredTeardown {
    fn run(self) {
        self.block_cache.detach();
        self.flush_queue.cleanup_standby_wal(&self.storage_path);
        let mut inner = self.inner.lock();
        inner.wal_writer.take();
        inner.versions.release_files();
        inner.active_memtable = Arc::new(MemTable::new());
        inner.immutable_memtables.clear();
        refresh_super_version(&self.super_version, &inner);
        drop(inner);
        self.dir_sync.lock().take();
        self.table_cache.close();
        drop(self.lock_file);
    }
}

#[derive(Default)]
struct WorkerLifecycleState {
    live_workers: usize,
    teardown_in_progress: bool,
    deferred: Option<DeferredTeardown>,
}

/// Tracks live background workers so dropping the final `Arc<DB>` from inside
/// a worker or scoped subcompaction callback can hand final resource/LOCK
/// teardown to the last exiting worker without self-joining or deadlocking.
#[derive(Default)]
pub(super) struct WorkerLifecycle {
    state: Mutex<WorkerLifecycleState>,
    all_exited: Condvar,
}

struct WorkerExitGuard(Arc<WorkerLifecycle>);

impl WorkerExitGuard {
    fn run(self, work: impl FnOnce()) {
        work();
        drop(self);
    }
}

impl WorkerLifecycle {
    fn enter(this: &Arc<Self>) -> WorkerExitGuard {
        this.state.lock().live_workers += 1;
        WorkerExitGuard(Arc::clone(this))
    }

    fn defer_teardown(&self, deferred: DeferredTeardown) {
        let run_now = {
            let mut state = self.state.lock();
            if state.live_workers == 0 {
                Some(deferred)
            } else {
                state.deferred = Some(deferred);
                None
            }
        };
        if let Some(deferred) = run_now {
            deferred.run();
        }
    }

    pub(super) fn wait_until_stopped(&self) {
        let mut state = self.state.lock();
        while state.live_workers != 0 || state.teardown_in_progress {
            self.all_exited.wait(&mut state);
        }
    }
}

impl Drop for WorkerExitGuard {
    fn drop(&mut self) {
        let deferred = {
            let mut state = self.0.state.lock();
            state.live_workers -= 1;
            if state.live_workers == 0 {
                let d = state.deferred.take();
                if d.is_some() {
                    state.teardown_in_progress = true;
                } else {
                    self.0.all_exited.notify_all();
                }
                d
            } else {
                None
            }
        };
        if let Some(deferred) = deferred {
            deferred.run();
            let mut state = self.0.state.lock();
            state.teardown_in_progress = false;
            self.0.all_exited.notify_all();
        }
    }
}

/// File numbers claimed by in-flight compaction picks, paired with a condvar
/// so the explicit full-compaction path can wait for in-flight picks to
/// install or discard instead of silently skipping their input files.
struct CompactionClaims {
    files: Mutex<HashSet<u64>>,
    released: Condvar,
}

impl CompactionClaims {
    fn new() -> Self {
        Self {
            files: Mutex::new(HashSet::new()),
            released: Condvar::new(),
        }
    }

    /// Lock the claimed-file set. Pick phases insert claims while holding
    /// this guard, atomically with the pick itself.
    fn lock(&self) -> MutexGuard<'_, HashSet<u64>> {
        self.files.lock()
    }

    /// Release claimed numbers and wake every waiter observing the set.
    fn release(&self, numbers: &[u64]) {
        let mut files = self.files.lock();
        for num in numbers {
            files.remove(num);
        }
        drop(files);
        self.released.notify_all();
    }

    /// Block until no compaction pick holds any claim. Callers must not hold
    /// `DB::inner` (claims are released only after install/discard, which
    /// needs that lock). Claims are RAII-released on every exit path — even a
    /// panicking compaction thread drops its `CompactionClaim` — so this
    /// terminates once in-flight work settles.
    fn wait_until_empty(&self) {
        let mut files = self.files.lock();
        while !files.is_empty() {
            self.released.wait(&mut files);
        }
    }
}

/// RAII release for file numbers claimed in `DB::compacting_files` while a
/// compaction pick is in flight (between `pick_compaction` and
/// `install_compaction`/discard). The numbers are inserted under the pick
/// phase's lock (atomically with the pick itself); this guard is constructed
/// immediately afterwards so that EVERY exit — success, compaction-I/O
/// error, install error — releases the claim. A leaked claim would
/// permanently exclude those files from all future picks
/// (`pick_l0_compaction` refuses to run while any current L0 file is
/// claimed), silently disabling L0 compaction for the life of the process
/// and hanging `CompactionClaims::wait_until_empty` forever.
struct CompactionClaim {
    claims: Arc<CompactionClaims>,
    numbers: Vec<u64>,
}

impl Drop for CompactionClaim {
    fn drop(&mut self) {
        self.claims.release(&self.numbers);
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn hint_wal_writeback(file: &fs::File, offset: u64, length: u64) {
    use std::os::fd::AsRawFd;
    let (Ok(offset), Ok(length)) = (i64::try_from(offset), i64::try_from(length)) else {
        return;
    };
    // SAFETY: the borrowed fd stays open through the syscall, and checked
    // conversions keep offset and length non-negative in `off64_t`. This
    // advisory call never substitutes for sync_data at a durability boundary.
    unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            offset,
            length,
            libc::SYNC_FILE_RANGE_WRITE,
        );
    }
}

/// The core database handle.
pub struct DB {
    read: Arc<ReadState>,
    path: PathBuf,
    storage_path: PathBuf,
    dir_sync: Arc<Mutex<Option<fs::File>>>,
    options: DbOptions,
    inner: Arc<Mutex<DBInner>>,
    /// Global sequence number (next to assign).
    sequence: Arc<AtomicU64>,
    /// Write queue and condvar for group commit.
    write_queue: Mutex<WriteQueueState>,
    write_cv: Condvar,
    flush_queue: Arc<FlushQueue>,
    flush_worker: Mutex<Option<JoinHandle<()>>>,
    synced_wal_seq: AtomicU64,
    maintenance: Mutex<MaintenanceState>,
    maintenance_cv: Condvar,
    block_cache: Arc<BlockCache>,
    table_cache: Arc<TableCache>,
    builds: Arc<SstBuilds>,
    rate_limiter: Arc<RateLimiter>,
    /// Shutdown flag for compaction threads.
    compaction_shutdown: Arc<AtomicBool>,
    /// Background compaction thread handles.
    compaction_handles: Mutex<Vec<JoinHandle<()>>>,
    worker_lifecycle: Arc<WorkerLifecycle>,
    /// File numbers currently claimed by an in-progress compaction pick
    /// (between `pick_compaction` and `install_compaction`/discard). Lets
    /// concurrent background compaction threads (multiple
    /// `max_background_compactions`) avoid redundantly picking and fully
    /// re-processing the same input files while one thread's pick is still
    /// doing unlocked I/O. `install_compaction`'s stale-input check already
    /// makes this safe on its own (the loser's output is discarded, never
    /// installed) — this only avoids wasting the loser's merge/compress/
    /// write work and rate-limiter budget. The paired condvar lets
    /// `force_compact_all` wait for in-flight picks to settle.
    compacting_files: Arc<CompactionClaims>,
    /// Cached L0 file count for fast write-throttle checks.
    /// Updated after flush/compaction. Avoids locking `inner` on every write.
    l0_file_count: Arc<AtomicUsize>,
    /// Tracks active snapshots for compaction safety.
    snapshot_list: Arc<SnapshotList>,
    /// Directory lock (`LOCK` file): on Unix, exclusive for writable handles,
    /// shared for read-only handles, and absent for a read-only immutable
    /// snapshot that does not contain `LOCK`. Other platforms retain the file
    /// handle but have no `flock`-based exclusion.
    /// Interior mutability lets explicit `close()` release it before `DB` drops.
    lock_file: Mutex<Option<DirLock>>,
    /// Keys registered for lazy deletion. Checked during compaction
    /// and dropped without writing tombstones.
    dead_keys: Arc<RwLock<HashSet<Vec<u8>>>>,
    /// State machine for automatic dead-key sweeping. New unique registrations
    /// at or above the threshold queue another pass; a request arriving during
    /// a pass is retained for the next pass.
    dead_key_sweep: Arc<DeadKeySweepScheduler>,
    /// Rotating start offset for the capped post-flush dead-key prune, so
    /// successive capped passes cover the whole set instead of repeatedly
    /// probing the same iteration prefix.
    dead_key_prune_cursor: AtomicUsize,
}

#[derive(Default)]
struct MaintenanceState {
    closing: bool,
    active: usize,
}

struct MaintenanceGuard<'a>(&'a DB);

impl Drop for MaintenanceGuard<'_> {
    fn drop(&mut self) {
        self.0.maintenance.lock().active -= 1;
        self.0.maintenance_cv.notify_all();
    }
}

struct DBInner {
    active_memtable: Arc<MemTable>,
    immutable_memtables: VecDeque<Arc<MemTable>>,
    ingest_ceiling: Arc<AtomicU64>,
    l0_install_epoch: u64,
    wal_writer: Option<WalWriter>,
    wal_number: u64,
    versions: VersionSet,
    closed_properties: Option<ClosedProperties>,
}

struct ClosedProperties {
    files_per_level: Vec<usize>,
    total_sst_size: u64,
    compaction_pending: bool,
}

/// Snapshot of the read-visible state: memtables + current version.
///
/// Models RocksDB's `SuperVersion`. Published atomically via `ArcSwap`
/// so readers never lock `inner` — they just load a single `Arc`.
/// Writers update this after every memtable/version change.
struct SuperVersion {
    active_memtable: Arc<MemTable>,
    immutable_memtables: Vec<Arc<MemTable>>,
    version: Arc<crate::manifest::version::Version>,
    ingest_ceiling: Arc<AtomicU64>,
}

/// Internal compaction filter that checks the dead-keys set before
/// delegating to an optional user-provided filter.
struct LazyDeleteFilter {
    dead_keys: Arc<RwLock<HashSet<Vec<u8>>>>,
    user_filter: Option<Arc<dyn CompactionFilter>>,
}

impl CompactionFilter for LazyDeleteFilter {
    fn filter(&self, level: usize, key: &[u8], value: &[u8]) -> CompactionFilterDecision {
        {
            let set = self.dead_keys.read();
            if set.contains(key) {
                return CompactionFilterDecision::Remove;
            }
        }
        if let Some(ref f) = self.user_filter {
            return f.filter(level, key, value);
        }
        CompactionFilterDecision::Keep
    }

    fn is_noop(&self) -> bool {
        // No-op only when there is no user filter to consult AND no key is
        // currently registered for lazy deletion. A dead key registered after a
        // trivial move is still removed by a later compaction that re-examines
        // the relocated file, so allowing the move here is safe.
        self.user_filter.as_ref().is_none_or(|f| f.is_noop()) && self.dead_keys.read().is_empty()
    }
}

/// Intersect query bounds before constructing or prefetching any source.
fn intersect_iter_bounds<'a>(
    options: &'a ReadOptions,
    lower: Option<&'a [u8]>,
    upper: Option<&'a [u8]>,
) -> (Option<&'a [u8]>, Option<&'a [u8]>) {
    let lower = match (options.iterate_lower_bound.as_deref(), lower) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    let upper = match (options.iterate_upper_bound.as_deref(), upper) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    (lower, upper)
}

#[cfg(test)]
thread_local! {
    static BEFORE_READ_VIEW: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    static AFTER_READ_VIEW: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

// Test-only seam for deterministically racing a concurrent write against
// `prune_settled_dead_keys`'s window between its unlocked absence probe and
// its locked re-verify-and-remove step. Thread-local (not a `DB` field) so
// it cannot leak across concurrently-running tests on other threads, and
// `#[cfg(test)]`-only so it never compiles into `test-utils` or release
// builds. Unused outside that one regression test.
#[cfg(test)]
thread_local! {
    static PRUNE_SETTLED_RACE_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    /// Counts `write_queue` acquisitions made by `prune_settled_dead_keys`'s
    /// re-verify phase, so a test can prove that phase is chunked instead of
    /// holding the writer barrier across every settled key. Thread-local for
    /// the same cross-test-isolation reason as the hook above.
    static PRUNE_SETTLED_LOCK_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// Test-only seam for `DB::open`'s compaction-worker spawn loop: forces the
// spawn at this 0-based worker index to be reported as failed, so a test
// can exercise partial-startup cleanup without needing real OS thread
// exhaustion. Thread-local for the same cross-test-isolation reason as
// `PRUNE_SETTLED_RACE_HOOK` above.
#[cfg(test)]
thread_local! {
    static FAIL_COMPACTION_SPAWN_AT: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    /// `DB::open` deposits its `compaction_shutdown` flag here on every
    /// call. Every worker thread — including one whose spawn is forced to
    /// fail above, since its (detached) real thread still shares this same
    /// `Arc` — holds a clone for its entire life, so a test can poll
    /// `Arc::strong_count` back down to the sink's own reference to confirm
    /// every worker actually exited instead of leaking.
    static LAST_COMPACTION_SHUTDOWN_FLAG: std::cell::RefCell<Option<Arc<AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
    /// Runs once right after a writable open acquires the directory flock.
    static AFTER_DIRECTORY_LOCK: std::cell::RefCell<Option<DirectoryLockHook>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
type DirectoryLockHook = Box<dyn FnOnce(&fs::File)>;

impl DB {
    /// Open an existing database without writing to its store directory.
    ///
    /// Valid records in residual WAL files are recovered into memory. Reads,
    /// iterators, snapshots, properties, [`close`](Self::close), and `Drop`
    /// leave the store unchanged. Fallible mutation APIs return
    /// [`ErrorKind::ReadOnly`](crate::ErrorKind::ReadOnly) before side effects;
    /// the existing infallible [`lazy_delete`](Self::lazy_delete) and
    /// [`lazy_delete_batch`](Self::lazy_delete_batch) methods are no-ops.
    ///
    /// This uses [`DbOptions::default`] for read/cache settings and adopts the
    /// persisted [`DbOptions::num_levels`] and [`DbOptions::prefix_len`] from
    /// the MANIFEST header. To customize cache or open-file settings, use
    /// [`open_read_only_with_options`](Self::open_read_only_with_options).
    ///
    /// # Locking and consistency
    ///
    /// On Unix, if the store contains `LOCK`, this takes a shared,
    /// non-blocking lock and rejects a concurrent writer. If `LOCK` is absent,
    /// opening proceeds unlocked so immutable snapshots remain usable; in that
    /// case the caller must keep the directory stable for this handle's entire
    /// lifetime. Platforms without Unix `flock` must also be treated as
    /// unlocked. Do not use an unlocked handle with a live writer: recovery
    /// could observe inconsistent MANIFEST, SST, and WAL states and return
    /// incomplete reads.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidArgument`](crate::ErrorKind::InvalidArgument)
    /// if `CURRENT` is absent or a cooperative lock conflicts. I/O, malformed
    /// metadata, or corrupt WAL/SST contents return their corresponding typed
    /// error.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_read_only_with_options(DbOptions::default(), path)
    }

    /// Open an existing database read-only with custom read/cache options.
    ///
    /// Writer-only options such as [`DbOptions::create_if_missing`],
    /// [`DbOptions::error_if_exists`], and the L0 write-throttling thresholds
    /// are ignored, and [`DbOptions::num_levels`] and [`DbOptions::prefix_len`]
    /// are adopted automatically from the persisted MANIFEST header rather than
    /// from `options`.
    ///
    /// Recovery, supported operations, errors, and the locking/stable-snapshot
    /// contract are identical to [`open_read_only`](Self::open_read_only).
    pub fn open_read_only_with_options(options: DbOptions, path: impl AsRef<Path>) -> Result<Self> {
        Self::open_impl(options, path, true)
    }

    /// Open or create a database.
    pub fn open(options: DbOptions, path: impl AsRef<Path>) -> Result<Self> {
        Self::open_impl(options, path, false)
    }

    fn open_impl(options: DbOptions, path: impl AsRef<Path>, read_only: bool) -> Result<Self> {
        let mut options = options;
        let requested_path = path.as_ref().to_path_buf();
        if requested_path.as_os_str().is_empty() {
            return Err(Error::invalid_argument("database path is empty"));
        }
        let path = if requested_path.is_relative() {
            std::env::current_dir().ctx()?.join(&requested_path)
        } else {
            requested_path.clone()
        };

        if !read_only
            && (!(2..=16).contains(&options.num_levels)
                || u32::try_from(options.prefix_len).is_err())
        {
            return Err(Error::invalid_argument(
                "num_levels must be in 2..=16 and prefix_len must fit u32",
            ));
        }

        // The slowdown window must precede the stop trigger. With
        // slowdown > stop, normal writes block at the stop trigger before
        // any slowdown delay can engage, so the configured window is
        // unreachable — reject the misconfiguration loudly instead of
        // silently reinterpreting it at write time.
        if !read_only && options.l0_slowdown_trigger > options.l0_stop_trigger {
            return Err(Error::invalid_argument(format!(
                "l0_slowdown_trigger ({}) must be <= l0_stop_trigger ({})",
                options.l0_slowdown_trigger, options.l0_stop_trigger
            )));
        }

        if !read_only && options.max_immutable_memtables == 0 {
            return Err(Error::invalid_argument(
                "max_immutable_memtables must be positive",
            ));
        }
        let num_compaction_threads = if read_only {
            0
        } else {
            options.max_background_compactions.max(1)
        };
        let mut compaction_handles = Vec::new();
        compaction_handles
            .try_reserve_exact(num_compaction_threads)
            .map_err(|err| {
                Error::invalid_argument(format!(
                    "cannot reserve handles for {num_compaction_threads} compaction workers"
                ))
                .with_source(err)
            })?;

        // A filter too large for even one key would fail every flush after
        // writes were acknowledged; reject it before accepting any.
        if !read_only {
            BloomFilter::checked_size("bloom filter", 1, options.bloom_bits_per_key).ctx()?;
        }

        if read_only {
            // Read-only open always means "open an existing DB". Normalize
            // the creation/existence flags so the stored options reflect the
            // capability actually granted to this handle.
            options.create_if_missing = false;
            options.error_if_exists = false;
        }
        if !path.join("CURRENT").try_exists().ctx()? {
            if !options.create_if_missing {
                return Err(Error::invalid_argument(format!(
                    "DB does not exist: {}",
                    path.display()
                )));
            }
            directory::create_durable(&path, fs::File::sync_all)
                .with_ctx(|| format!("preparing database directory {}", path.display()))?;
        }

        if !read_only && options.error_if_exists {
            let current = path.join("CURRENT");
            if current.try_exists().ctx()? {
                return Err(Error::invalid_argument(format!(
                    "DB already exists: {}",
                    path.display()
                )));
            }
        }

        // Keep every later flush, compaction and cleanup in the directory
        // selected at open, even if a symlink in the caller's path changes.
        let path = fs::canonicalize(&path).ctx()?;

        // Lock before reading CURRENT/MANIFEST/WAL. Writable handles create
        // the lock file and take LOCK_EX. Read-only handles open an existing
        // lock file without write intent and take LOCK_SH; immutable snapshots
        // that do not contain LOCK proceed unlocked by design.
        let lock_file = if read_only {
            let lock_path = path.join("LOCK");
            match OpenOptions::new().read(true).open(&lock_path) {
                Ok(file) => {
                    #[cfg(unix)]
                    {
                        use std::os::fd::AsRawFd;
                        // SAFETY: flock only observes the valid fd borrowed from `file`.
                        let ret =
                            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) };
                        if ret != 0 {
                            let err = io::Error::last_os_error();
                            return Err(Error::invalid_argument(format!(
                                "failed to lock DB directory {} for reading: {} \
                                 (is a writer using it?)",
                                path.display(),
                                err
                            )));
                        }
                    }
                    Some(DirLock(file))
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => None,
                Err(e) => return Err(e).ctx(),
            }
        } else {
            let lock_path = path.join("LOCK");
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&lock_path)
                .ctx()?;
            #[cfg(unix)]
            {
                use std::os::fd::AsRawFd;
                // SAFETY: flock only observes the valid fd borrowed from `file`.
                let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                if ret != 0 {
                    let err = io::Error::last_os_error();
                    return Err(Error::invalid_argument(format!(
                        "failed to lock DB directory {}: {} (is another process using it?)",
                        path.display(),
                        err
                    )));
                }
            }
            #[cfg(test)]
            if let Some(hook) = AFTER_DIRECTORY_LOCK.with(|hook| hook.borrow_mut().take()) {
                hook(&file);
            }
            Some(DirLock(file))
        };

        // Create caches and infra
        let block_cache = Arc::new(match options.block_cache {
            // Shared pool supplied by the caller: attach as a member
            // (block_cache_capacity is ignored — capacity is the pool's).
            Some(ref pool) => pool.attach(),
            // Private single-member pool — the historical behavior.
            None => BlockCache::new(options.block_cache_capacity),
        });
        struct DetachOnOpenErr(Option<Arc<BlockCache>>);
        impl Drop for DetachOnOpenErr {
            fn drop(&mut self) {
                if let Some(ref cache) = self.0 {
                    cache.detach();
                }
            }
        }
        let mut detach_on_err = DetachOnOpenErr(Some(block_cache.clone()));
        let builds = Arc::new(SstBuilds::default());
        let rate_limiter = Arc::new(RateLimiter::new(options.rate_limiter_bytes_per_sec));
        let stats = Arc::new(DbStats::new());
        let table_cache = Arc::new(TableCache::new_with_stats(
            &path,
            options.max_open_files,
            Some(block_cache.clone()),
            Some(stats.clone()),
        ));

        // Open or create VersionSet (handles MANIFEST)
        let mut versions = if read_only {
            VersionSet::recover_db_read_only_with_cache(&path, Some(table_cache.clone())).ctx()?
        } else if path.join("CURRENT").try_exists().ctx()? {
            // A competing creator may have finished between preflight and LOCK.
            if options.error_if_exists {
                return Err(Error::invalid_argument(format!(
                    "DB already exists: {}",
                    path.display()
                )));
            }
            VersionSet::recover_db_with_cache(
                &path,
                options.num_levels,
                options.prefix_len,
                Some(table_cache.clone()),
            )
            .ctx()?
        } else if options.create_if_missing {
            VersionSet::create_db_with_cache(
                &path,
                options.num_levels,
                options.prefix_len,
                Some(table_cache.clone()),
            )
            .ctx()?
        } else {
            return Err(Error::invalid_argument(format!(
                "DB does not exist: {}",
                path.display()
            )));
        };
        (options.num_levels, options.prefix_len) = versions.geometry();
        let mut max_sequence = versions.last_sequence();

        // The zero log number belongs to a fresh MANIFEST before the first
        // WAL is installed. Every later floor names a dir-synced WAL.
        let log_number = versions.log_number();
        if log_number != 0
            && !path
                .join(format!("{log_number:06}.wal"))
                .try_exists()
                .ctx()?
        {
            return Err(Error::corruption(format!(
                "MANIFEST references missing WAL {log_number:06}.wal"
            )));
        }

        // Recover from any WAL files not yet flushed
        let mut active_memtable = Arc::new(MemTable::new());
        let mut wal_numbers: Vec<u64> = Vec::new();
        let mut max_disk_file_number = 0u64;
        for entry in fs::read_dir(&path).ctx()? {
            let entry = entry.ctx()?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            // Track the highest file number present on disk across WAL/SST/MANIFEST files.
            if let Some(num) = name
                .strip_suffix(".wal")
                .or_else(|| name.strip_suffix(".sst"))
                .or_else(|| name.strip_prefix("MANIFEST-"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                max_disk_file_number = max_disk_file_number.max(num);
            }
            if let Some(num_str) = name.strip_suffix(".wal")
                && let Ok(num) = num_str.parse::<u64>()
            {
                // Only recover WAL files newer than what's recorded in MANIFEST
                if num >= log_number {
                    wal_numbers.push(num);
                }
            }
        }
        wal_numbers.sort();

        // Never hand out a file number that already exists on disk. A crash
        // during flush can create a WAL whose number was not yet persisted in
        // the MANIFEST; if recovery reused that number for the fresh WAL and
        // then deleted the old WALs, it would unlink the live active WAL and
        // lose subsequently-acknowledged writes.
        versions
            .file_numbers
            .advance_to(max_disk_file_number.saturating_add(1));

        let recoverable_tail_wal_num = wal_numbers.iter().rev().copied().find(|wal_num| {
            let wal_path = path.join(format!("{:06}.wal", wal_num));
            match fs::metadata(&wal_path) {
                Ok(meta) => meta.len() != 0,
                Err(e) => {
                    tracing::warn!(
                        "could not stat WAL {} while selecting recoverable tail: {}",
                        wal_num,
                        e
                    );
                    true
                }
            }
        });
        // Stage bounded recovery outputs until every relevant WAL has been
        // validated. A crash before the single install replays the original WALs.
        let mut recovery_outputs = Vec::new();
        let file_numbers = versions.file_numbers.clone();
        let recovery_path = path.clone();
        let staged = PendingSsts::new(&recovery_path, &file_numbers);
        let mut spill = |mem: &mut Arc<MemTable>| -> Result<()> {
            let outputs = Self::write_memtable_ssts(
                mem,
                &path,
                &|| l0_build_opts(&options),
                &file_numbers,
                &[],
            )?;
            staged.track(outputs.iter().map(|(n, _)| *n));
            recovery_outputs.extend(outputs.into_iter().map(flush::file_metadata));
            *mem = Arc::new(MemTable::new());
            Ok(())
        };
        let mut last_wal_seq = 0u64;
        for wal_num in &wal_numbers {
            let wal_path = path.join(format!("{:06}.wal", wal_num));
            let mut reader = WalReader::new(&wal_path).ctx()?;
            loop {
                match reader.read_record() {
                    Ok(Some(data)) => {
                        Self::replay_wal_record(
                            &data,
                            &active_memtable,
                            &mut last_wal_seq,
                            &mut max_sequence,
                        )
                        .ctx()?;
                        if !read_only
                            && active_memtable.approximate_size() >= options.write_buffer_size
                        {
                            spill(&mut active_memtable)?;
                        }
                    }
                    Ok(None) => break,
                    // A torn tail (crash mid-append) can surface as a corrupt
                    // record followed only by zero padding / a zero-extended
                    // file tail. Tolerate exactly that case for the highest
                    // non-empty recovered WAL: it was the active append target at
                    // crash. Empty higher WALs can be recovery-created orphans
                    // from a crash before the new log_number reached MANIFEST.
                    //
                    // Corruption in any earlier WAL, or corruption followed by
                    // non-zero data, is NOT a torn active tail. Silently
                    // recovering the prefix would drop committed records while
                    // still replaying newer WALs (a causal hole), and the
                    // post-recovery cleanup would then delete the WAL. Fail the
                    // open loudly instead and preserve the file for inspection.
                    Err(e) => {
                        // Only structural short-reads (partial header/payload/
                        // trailer / zero-extended tip) may be a torn active
                        // tail. Checksum, type, and other semantic failures fail
                        // closed: an untrusted length can already have skipped
                        // later valid records, so zero bytes past the advanced
                        // position are not proof of a safe prefix.
                        if Some(*wal_num) == recoverable_tail_wal_num
                            && reader.last_error_is_truncation()
                            && reader.rest_is_zero_padding().unwrap_or(false)
                        {
                            tracing::warn!(
                                "WAL {} has corrupt tail, stopping replay: {}",
                                wal_num,
                                e
                            );
                            break;
                        }
                        return Err(e).with_ctx(|| {
                            format!(
                                "WAL {:06} is corrupt before the recoverable active tail; \
                             refusing prefix recovery to avoid silent data loss",
                                wal_num
                            )
                        });
                    }
                }
            }
            // Admission bounds apply to each original memtable independently.
            // Do not merge a same-key run across generations during replay.
            if !read_only && !active_memtable.is_empty() {
                spill(&mut active_memtable)?;
            }
        }

        let (wal_writer, wal_number) = if read_only {
            // Preserve the recovered WAL boundary as descriptive state. The
            // replayed memtable remains live and is published below.
            (None, versions.log_number())
        } else {
            // Create the fresh WAL up front so its number can be recorded in the
            // SAME MANIFEST edit that installs the recovered SST. Otherwise a crash
            // between installing the SST and advancing `log_number` would replay the
            // same WALs again and create a duplicate L0 file with identical keys.
            let wal_number = versions.file_numbers.allocate().ctx()?;
            let wal_path = path.join(format!("{:06}.wal", wal_number));
            let wal_writer = WalWriter::new(&wal_path).ctx()?;

            let mut edit = VersionEdit::new();
            edit.set_log_number(wal_number);
            edit.set_last_sequence(max_sequence);
            edit.set_next_file_number(versions.next_file_number());
            if recovery_outputs.is_empty() {
                versions.apply_metadata(edit).ctx()?;
            } else {
                let synced =
                    sync_new_sst_parent_dir(&versions.dir_sync_handle(), &versions.poison_flag())?;
                for meta in recovery_outputs {
                    edit.add_file(0, meta);
                }
                versions.log_and_apply_synced(edit, synced).ctx()?;
                staged.keep();
            }
            versions.sync_manifest().ctx()?;

            // Safe to clean up obsolete files now — the new log_number is durable
            // (so old WALs will never be replayed even if we crash here) and the
            // recovered version set defines the complete live SST set.
            Self::remove_orphan_files(&path, &versions);
            (Some(wal_writer), wal_number)
        };

        let next_sequence = max_sequence.checked_add(1).ok_or_else(|| {
            Error::invalid_argument("sequence number space exhausted".to_string())
        })?;
        let sequence_start = Arc::new(AtomicU64::new(next_sequence));
        let committed_sequence = Arc::new(AtomicU64::new(max_sequence));

        let manifest_poisoned = versions.poison_flag();
        let dir_sync = versions.dir_sync_handle();

        let inner = Arc::new(Mutex::new(DBInner {
            active_memtable,
            immutable_memtables: VecDeque::new(),
            ingest_ceiling: Arc::new(AtomicU64::new(MAX_SEQUENCE_NUMBER)),
            l0_install_epoch: 0,
            wal_writer,
            wal_number,
            versions,
            closed_properties: None,
        }));

        // Spawn background compaction threads
        let compaction_shutdown = Arc::new(AtomicBool::new(false));
        let compaction_notify = Arc::new((Mutex::new(false), Condvar::new()));
        #[cfg(test)]
        LAST_COMPACTION_SHUTDOWN_FLAG
            .with(|sink| *sink.borrow_mut() = Some(compaction_shutdown.clone()));
        let has_bg_error = Arc::new(AtomicBool::new(false));
        let bg_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let compacting_files = Arc::new(CompactionClaims::new());

        // Wrap the user's compaction filter with lazy-delete support.
        let dead_keys = Arc::new(RwLock::new(HashSet::new()));
        {
            let lazy_filter = LazyDeleteFilter {
                dead_keys: dead_keys.clone(),
                user_filter: options.compaction_filter.take(),
            };
            options.compaction_filter = Some(Arc::new(lazy_filter));
        }
        let dead_key_sweep = Arc::new(DeadKeySweepScheduler::new());

        let (l0_file_count, super_version) = {
            let g = inner.lock();
            let version = g.versions.current();
            let files = version.l0_file_count();
            let l0 = Arc::new(AtomicUsize::new(files));
            #[cfg(feature = "bench-metrics")]
            {
                stats.l0_profile.lock().peak_installed_l0_files = files as u64;
            }
            let sv = Arc::new(ArcSwap::from_pointee(SuperVersion {
                active_memtable: g.active_memtable.clone(),
                immutable_memtables: g.immutable_memtables.iter().cloned().collect(),
                version,
                ingest_ceiling: g.ingest_ceiling.clone(),
            }));
            (l0, sv)
        };
        let read_compaction_hints = Arc::new(Mutex::new(Vec::<CompactionHint>::new()));
        let snapshot_list = Arc::new(SnapshotList::new());
        let flush_queue = Arc::new(FlushQueue::default());
        let worker_lifecycle = Arc::new(WorkerLifecycle::default());
        for i in 0..num_compaction_threads {
            let bg_flush_queue = flush_queue.clone();
            let bg_inner = Arc::clone(&inner);
            let bg_path = path.clone();
            let bg_options = options.clone();
            let bg_table_cache = table_cache.clone();
            let bg_block_cache = block_cache.clone();
            let bg_builds = builds.clone();
            let bg_rate_limiter = rate_limiter.clone();
            let bg_stats = stats.clone();
            let bg_l0_count = l0_file_count.clone();
            let bg_sv = super_version.clone();
            let shutdown = compaction_shutdown.clone();
            let notify = compaction_notify.clone();
            let bg_hints = read_compaction_hints.clone();
            let bg_snapshot_list = snapshot_list.clone();
            let bg_has_error = has_bg_error.clone();
            let bg_error_msg = bg_error.clone();
            let bg_compacting_files = compacting_files.clone();
            let bg_dead_key_sweep = dead_key_sweep.clone();
            let bg_manifest_poisoned = manifest_poisoned.clone();
            let bg_dir_sync = dir_sync.clone();
            let worker_exit = WorkerLifecycle::enter(&worker_lifecycle);

            let handle = thread::Builder::new()
                .name(format!("mmdb-compaction-{}", i))
                .spawn(move || {
                    worker_exit.run(move || {
                        // Helper: set background error and log it.
                        let set_error = |msg: String| {
                            tracing::error!("{}", msg);
                            *bg_error_msg.lock() = Some(msg.clone());
                            bg_has_error.store(true, Ordering::Release);
                            bg_flush_queue.fail(Error::background(msg));
                        };

                        let mut retry_l0 = false;
                        loop {
                            // Wait for notification
                            {
                                let (lock, cvar) = &*notify;
                                let mut has_work = lock.lock();
                                while !*has_work && !shutdown.load(Ordering::Acquire) {
                                    cvar.wait(&mut has_work);
                                }
                                *has_work = false; // consumed
                            }
                            if shutdown.load(Ordering::Acquire) {
                                break;
                            }

                            // Wrap compaction work in catch_unwind to prevent
                            // thread death from panics in corrupt data paths.
                            let result = catch_unwind(AssertUnwindSafe(
                                || -> std::result::Result<(), String> {
                                    // Run pending organic compactions first (L0 and byte-budget
                                    // levels take strict precedence over optional read hints),
                                    // and re-check organic budgets whenever a hint installs an
                                    // edit that may push its target level over budget.
                                    loop {
                                        loop {
                                            if shutdown.load(Ordering::Acquire) {
                                                return Ok(());
                                            }
                                            // Phase 1: pick and claim (short lock)
                                            let pick = {
                                                let inner = bg_inner.lock();
                                                let version = inner.versions.current();
                                                let mut claimed = bg_compacting_files.lock();
                                                match LeveledCompaction::pick_compaction(
                                                    &version,
                                                    &bg_options,
                                                    &claimed,
                                                ) {
                                                    Some(task) => {
                                                        let l0_inputs: Vec<u64> = if task.level == 0
                                                        {
                                                            task.input_files_level
                                                                .iter()
                                                                .map(|f| f.meta.number)
                                                                .collect()
                                                        } else {
                                                            Vec::new()
                                                        };
                                                        let file_numbers =
                                                            inner.versions.file_numbers.clone();
                                                        let all_inputs: Vec<_> = task
                                                            .input_files_level
                                                            .iter()
                                                            .chain(task.input_files_next.iter())
                                                            .cloned()
                                                            .collect();
                                                        let is_bottom =
                                                            LeveledCompaction::is_bottommost_level(
                                                                &version,
                                                                task.level,
                                                                bg_options.num_levels,
                                                                &all_inputs,
                                                            );
                                                        let claimed_numbers: Vec<u64> = all_inputs
                                                            .iter()
                                                            .map(|f| f.meta.number)
                                                            .collect();
                                                        claimed.extend(
                                                            claimed_numbers.iter().copied(),
                                                        );
                                                        let active_snaps =
                                                            bg_snapshot_list.as_sorted_vec();
                                                        Some((
                                                            task,
                                                            file_numbers,
                                                            l0_inputs,
                                                            is_bottom,
                                                            active_snaps,
                                                            claimed_numbers,
                                                        ))
                                                    }
                                                    None => None,
                                                }
                                            };

                                            let Some((
                                                task,
                                                file_numbers,
                                                l0_inputs,
                                                is_bottom,
                                                active_snaps,
                                                claimed_numbers,
                                            )) = pick
                                            else {
                                                break;
                                            };
                                            let _claim = CompactionClaim {
                                                claims: Arc::clone(&bg_compacting_files),
                                                numbers: claimed_numbers,
                                            };

                                            // Phase 2: I/O (no lock held)
                                            let ctx = CompactionContext {
                                                db_path: &bg_path,
                                                dir_sync: &bg_dir_sync,
                                                manifest_poisoned: &bg_manifest_poisoned,
                                                options: &bg_options,
                                                builds: &bg_builds,
                                                rate_limiter: Some(&bg_rate_limiter),
                                                stats: Some(&bg_stats),
                                                active_snapshots: &active_snaps,
                                            };
                                            let output = LeveledCompaction::execute_compaction_io(
                                                &ctx,
                                                &task,
                                                &file_numbers,
                                                is_bottom,
                                            )
                                            .map_err(|e| format!("compaction error: {}", e))?;

                                            bg_table_cache.prewarm(
                                                output
                                                    .edit
                                                    .new_files
                                                    .iter()
                                                    .map(|(_, meta)| meta.number),
                                            );

                                            // Phase 3: validate and install the directory-synced outputs.
                                            let cleanup = {
                                                let mut inner = bg_inner.lock();
                                                let cleanup =
                                                    LeveledCompaction::install_compaction(
                                                        output,
                                                        &mut inner.versions,
                                                        Some(&bg_table_cache),
                                                        Some(&bg_block_cache),
                                                        &bg_path,
                                                        Some(&bg_stats),
                                                        !bg_snapshot_list.is_empty(),
                                                    )
                                                    .map_err(|e| {
                                                        format!("compaction install error: {}", e)
                                                    })?;
                                                if !l0_inputs.is_empty() {
                                                    let current = inner.versions.current();
                                                    for num in &l0_inputs {
                                                        if !current
                                                            .level_files(0)
                                                            .iter()
                                                            .any(|tf| tf.meta.number == *num)
                                                        {
                                                            bg_block_cache.unpin_file(*num);
                                                        }
                                                    }
                                                }
                                                bg_l0_count.store(
                                                    inner.versions.current().l0_file_count(),
                                                    Ordering::Relaxed,
                                                );
                                                refresh_super_version(&bg_sv, &inner);
                                                cleanup
                                            };
                                            // Phase 4: sync manifest + delete old SSTs (no lock)
                                            if let Err(e) = {
                                                let manifest_handle =
                                                    bg_inner.lock().versions.manifest_sync_handle();
                                                confirm_manifest_durable(
                                                    &manifest_handle,
                                                    &bg_manifest_poisoned,
                                                )
                                            } {
                                                return Err(format!("manifest sync error: {}", e));
                                            }
                                            LeveledCompaction::run_post_compaction_cleanup(
                                                &cleanup, &bg_path,
                                            );
                                        }

                                        // Drain read-compaction hints only after organic backlog clears.
                                        let mut hint_installed = false;
                                        let hints: Vec<CompactionHint> =
                                            mem::take(&mut *bg_hints.lock());
                                        if !hints.is_empty() {
                                            for hint in &hints {
                                                if shutdown.load(Ordering::Acquire) {
                                                    return Ok(());
                                                }
                                                // Evaluate user filter `is_noop()` outside `DB::inner`,
                                                // covered by the SST callback re-entrancy guard.
                                                let allow_trivial_move = {
                                                    let _build = bg_builds.enter();
                                                    bg_options
                                                        .compaction_filter
                                                        .as_ref()
                                                        .is_none_or(|f| f.is_noop())
                                                };
                                                let pick = {
                                                    let inner = bg_inner.lock();
                                                    let version = inner.versions.current();
                                                    let mut claimed = bg_compacting_files.lock();
                                                    LeveledCompaction::pick_compaction_for_hint(
                                                        &version,
                                                        &bg_options,
                                                        hint,
                                                        &claimed,
                                                        allow_trivial_move,
                                                    )
                                                    .map(|task| {
                                                        let file_numbers =
                                                            inner.versions.file_numbers.clone();
                                                        let all_inputs: Vec<_> = task
                                                            .input_files_level
                                                            .iter()
                                                            .chain(task.input_files_next.iter())
                                                            .cloned()
                                                            .collect();
                                                        let is_bottom =
                                                            LeveledCompaction::is_bottommost_level(
                                                                &version,
                                                                task.level,
                                                                bg_options.num_levels,
                                                                &all_inputs,
                                                            );
                                                        let claimed_numbers: Vec<u64> = all_inputs
                                                            .iter()
                                                            .map(|f| f.meta.number)
                                                            .collect();
                                                        claimed.extend(
                                                            claimed_numbers.iter().copied(),
                                                        );
                                                        let active_snaps =
                                                            bg_snapshot_list.as_sorted_vec();
                                                        (
                                                            task,
                                                            file_numbers,
                                                            is_bottom,
                                                            active_snaps,
                                                            claimed_numbers,
                                                        )
                                                    })
                                                };

                                                if let Some((
                                                    task,
                                                    file_numbers,
                                                    is_bottom,
                                                    active_snaps,
                                                    claimed_numbers,
                                                )) = pick
                                                {
                                                    let _claim = CompactionClaim {
                                                        claims: Arc::clone(&bg_compacting_files),
                                                        numbers: claimed_numbers,
                                                    };
                                                    let ctx = CompactionContext {
                                                        db_path: &bg_path,
                                                        dir_sync: &bg_dir_sync,
                                                        manifest_poisoned: &bg_manifest_poisoned,
                                                        options: &bg_options,
                                                        builds: &bg_builds,
                                                        rate_limiter: Some(&bg_rate_limiter),
                                                        stats: Some(&bg_stats),
                                                        active_snapshots: &active_snaps,
                                                    };
                                                    let output =
                                                        LeveledCompaction::execute_compaction_io(
                                                            &ctx,
                                                            &task,
                                                            &file_numbers,
                                                            is_bottom,
                                                        )
                                                        .map_err(|e| {
                                                            format!("hint compaction error: {}", e)
                                                        })?;

                                                    bg_table_cache.prewarm(
                                                        output
                                                            .edit
                                                            .new_files
                                                            .iter()
                                                            .map(|(_, meta)| meta.number),
                                                    );

                                                    let cleanup = {
                                                        let mut inner = bg_inner.lock();
                                                        let cleanup =
                                                        LeveledCompaction::install_compaction(
                                                            output,
                                                            &mut inner.versions,
                                                            Some(&bg_table_cache),
                                                            Some(&bg_block_cache),
                                                            &bg_path,
                                                            Some(&bg_stats),
                                                            !bg_snapshot_list.is_empty(),
                                                        )
                                                        .map_err(|e| {
                                                            format!(
                                                                "hint compaction install error: {}",
                                                                e
                                                            )
                                                        })?;
                                                        bg_l0_count.store(
                                                            inner
                                                                .versions
                                                                .current()
                                                                .l0_file_count(),
                                                            Ordering::Relaxed,
                                                        );
                                                        refresh_super_version(&bg_sv, &inner);
                                                        cleanup
                                                    };
                                                    if let Err(e) = {
                                                        let manifest_handle = bg_inner
                                                            .lock()
                                                            .versions
                                                            .manifest_sync_handle();
                                                        confirm_manifest_durable(
                                                            &manifest_handle,
                                                            &bg_manifest_poisoned,
                                                        )
                                                    } {
                                                        return Err(format!(
                                                            "manifest sync error: {}",
                                                            e
                                                        ));
                                                    }
                                                    hint_installed |= cleanup.installed;
                                                    LeveledCompaction::run_post_compaction_cleanup(
                                                        &cleanup, &bg_path,
                                                    );
                                                }
                                            }
                                        }
                                        if !hint_installed {
                                            break;
                                        }
                                    }

                                    // A pending sweep stays queued while snapshots
                                    // are active. Once runnable, process deeper
                                    // levels first so removing older copies can make
                                    // shallower copies eligible in the same pass.
                                    loop {
                                        if shutdown.load(Ordering::Acquire)
                                            || !bg_dead_key_sweep.is_pending()
                                            || !bg_snapshot_list.is_empty()
                                            || !bg_dead_key_sweep.try_start()
                                        {
                                            break;
                                        }

                                        let mut blocked_by_snapshot = false;
                                        let mut deferred_l0 = false;
                                        for level in (0..bg_options.num_levels).rev() {
                                            if shutdown.load(Ordering::Acquire) {
                                                bg_dead_key_sweep.finish(true);
                                                return Ok(());
                                            }
                                            blocked_by_snapshot |= !bg_snapshot_list.is_empty();
                                            let ctx = CompactionContext {
                                                db_path: &bg_path,
                                                dir_sync: &bg_dir_sync,
                                                manifest_poisoned: &bg_manifest_poisoned,
                                                options: &bg_options,
                                                builds: &bg_builds,
                                                rate_limiter: Some(&bg_rate_limiter),
                                                stats: Some(&bg_stats),
                                                active_snapshots: &[],
                                            };
                                            let rewrite = LevelRewrite {
                                                inner: &bg_inner,
                                                snapshots: &bg_snapshot_list,
                                                claims: &bg_compacting_files,
                                                table_cache: &bg_table_cache,
                                                block_cache: &bg_block_cache,
                                                super_version: &bg_sv,
                                                l0_count: &bg_l0_count,
                                            };
                                            let swept = rewrite
                                                .run(level, &ctx, level == 0 && retry_l0)
                                                .map_err(|e| {
                                                    format!("dead-key sweep error at L{level}: {e}")
                                                })?;
                                            if level == 0 {
                                                deferred_l0 = !swept;
                                                retry_l0 = !swept && !retry_l0;
                                            }
                                            blocked_by_snapshot |= !bg_snapshot_list.is_empty();
                                        }
                                        #[cfg(test)]
                                        if deferred_l0 {
                                            let hook = bg_dead_key_sweep
                                                .before_deferred_finish
                                                .lock()
                                                .take();
                                            if let Some(hook) = hook {
                                                hook();
                                            }
                                        }
                                        bg_dead_key_sweep
                                            .finish(blocked_by_snapshot || deferred_l0);
                                        // Publish Pending before checking whether the
                                        // flush is still in flight. Another worker may
                                        // have consumed its notification while this
                                        // sweep was Running. If it already installed,
                                        // retry here; otherwise its install follows
                                        // this locked check and sends a new wake.
                                        if deferred_l0
                                            && !bg_inner.lock().immutable_memtables.is_empty()
                                        {
                                            break;
                                        }
                                    }
                                    Ok(())
                                },
                            ));

                            match result {
                                Ok(Ok(())) => {
                                    #[cfg(test)]
                                    bg_dead_key_sweep
                                        .completed_passes
                                        .fetch_add(1, Ordering::Release);
                                }
                                Ok(Err(msg)) => {
                                    set_error(msg);
                                    break;
                                }
                                Err(panic_payload) => {
                                    let msg = if let Some(s) =
                                        panic_payload.downcast_ref::<String>()
                                    {
                                        format!("compaction thread panicked: {}", s)
                                    } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                                        format!("compaction thread panicked: {}", s)
                                    } else {
                                        "compaction thread panicked with unknown payload"
                                            .to_string()
                                    };
                                    set_error(msg);
                                    break;
                                }
                            }
                        }
                    });
                });
            // Test-only: force this iteration to be reported as a failed
            // spawn. Any real handle just created above is dropped
            // (detached) rather than tracked — its thread still shares
            // `compaction_shutdown`/`compaction_notify` with every other
            // worker, so it exits once the `Err` branch below signals
            // shutdown, same as the genuinely-prior workers.
            #[cfg(test)]
            let handle = if FAIL_COMPACTION_SPAWN_AT.with(|f| f.get()) == Some(i) {
                Err(io::Error::other(
                    "test-injected compaction thread spawn failure",
                ))
            } else {
                handle
            };
            match handle {
                Ok(handle) => compaction_handles.push(handle),
                Err(e) => {
                    // Partial startup failure: workers 0..i already spawned
                    // are running and blocked on `compaction_notify`, each
                    // holding Arc clones of `inner`/`super_version`/etc.
                    // Returning here without unwinding them would leak both
                    // the threads and everything they keep alive for the
                    // rest of the process — signal and wake them, then join
                    // before propagating the spawn error.
                    {
                        let (lock, cvar) = &*compaction_notify;
                        let _guard = lock.lock();
                        compaction_shutdown.store(true, Ordering::Release);
                        cvar.notify_all();
                    }
                    for h in compaction_handles.drain(..) {
                        let _ = h.join();
                    }
                    return Err(e).with_ctx(|| "failed to spawn compaction thread");
                }
            }
        }

        let db = Self {
            read: Arc::new(ReadState {
                read_only,
                committed_sequence,
                closed: AtomicBool::new(false),
                db_id: NEXT_DB_ID
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                    .expect("database ID space exhausted"),
                has_bg_error,
                bg_error,
                manifest_poisoned,
                stats,
                compaction_notify,
                super_version,
                read_compaction_hints,
                read_counter: AtomicU64::new(0),
            }),
            path: requested_path,
            storage_path: path,
            dir_sync,
            options,
            inner,
            sequence: sequence_start.clone(),
            write_queue: Mutex::new(WriteQueueState {
                waiting_barriers: 0,
                queue: VecDeque::new(),
                leader_active: false,
            }),
            write_cv: Condvar::new(),
            flush_queue: flush_queue.clone(),
            flush_worker: Mutex::new(None),
            synced_wal_seq: AtomicU64::new(max_sequence),
            maintenance: Mutex::new(MaintenanceState::default()),
            maintenance_cv: Condvar::new(),

            block_cache,
            table_cache,
            builds,
            rate_limiter,
            compaction_shutdown,
            compaction_handles: Mutex::new(compaction_handles),
            worker_lifecycle: Arc::clone(&worker_lifecycle),
            compacting_files,
            l0_file_count,

            snapshot_list,
            lock_file: Mutex::new(lock_file),
            dead_keys,
            dead_key_sweep,
            dead_key_prune_cursor: AtomicUsize::new(0),
        };
        detach_on_err.0 = None;

        if !read_only {
            let ctx = db.flush_context();
            let worker_exit = WorkerLifecycle::enter(&worker_lifecycle);
            *db.flush_worker.lock() = Some(
                thread::Builder::new()
                    .name("mmdb-flush".into())
                    .spawn(move || worker_exit.run(move || flush_queue.run(&ctx)))
                    .ctx()?,
            );
        }

        // Kick the background compaction threads once at startup. A DB
        // opened with a pre-existing L0 backlog (e.g. WAL-recovery SSTs
        // accumulated across short-lived processes) would otherwise not
        // compact until the first memtable flush of THIS process signals
        // the background thread — while every write pays the L0-slowdown
        // penalty (and small workloads may never fill a memtable at all,
        // making the stall permanent). The thread re-checks
        // `pick_compaction` and goes back to sleep if there is nothing
        // to do, so an unconditional signal is free.
        if !read_only {
            db.read.signal_compaction();
        }

        Ok(db)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with_options(&WriteOptions::default(), key, value)
    }

    pub fn put_with_options(
        &self,
        write_options: &WriteOptions,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.check_writable().ctx()?;
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_batch_inner(batch, write_options)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_with_options(&WriteOptions::default(), key)
    }

    pub fn delete_with_options(&self, write_options: &WriteOptions, key: &[u8]) -> Result<()> {
        self.check_writable().ctx()?;
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_batch_inner(batch, write_options)
    }

    /// Delete all keys in the range [begin, end).
    /// The combined endpoint length must not exceed [`crate::MAX_RANGE_DELETE_SIZE`].
    /// When 6,290,928 distinct snapshot sequences are live, every batch containing
    /// a nonempty range deletion returns [`ErrorKind::InvalidArgument`](crate::ErrorKind::InvalidArgument)
    /// before WAL append. This reserves room for the latest visible deletion and
    /// both maximum-length endpoints in a single range-deletion leaf.
    pub fn delete_range(&self, begin: &[u8], end: &[u8]) -> Result<()> {
        self.delete_range_with_options(&WriteOptions::default(), begin, end)
    }

    pub fn delete_range_with_options(
        &self,
        write_options: &WriteOptions,
        begin: &[u8],
        end: &[u8],
    ) -> Result<()> {
        self.check_writable().ctx()?;
        let mut batch = WriteBatch::new();
        batch.delete_range(begin, end);
        self.write_batch_inner(batch, write_options)
    }

    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.write_with_options(&WriteOptions::default(), batch)
    }

    pub fn write_with_options(
        &self,
        write_options: &WriteOptions,
        batch: WriteBatch,
    ) -> Result<()> {
        self.check_writable().ctx()?;
        batch.check_valid().ctx()?;
        if batch.is_empty() {
            return Ok(());
        }
        self.write_batch_inner(batch, write_options)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_with_options(&ReadOptions::default(), key)
    }

    pub fn get_with_options(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read.point_lookup(options, key)
    }

    /// Look up a visible value and retain its backing memory.
    pub fn get_pinned(&self, key: &[u8]) -> Result<Option<PinnedSlice>> {
        self.get_pinned_with_options(&ReadOptions::default(), key)
    }

    /// Check visibility at the current committed sequence without copying the value.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool> {
        self.contains_key_with_options(&ReadOptions::default(), key)
    }

    /// Check existence using the supplied snapshot and cache policy.
    pub fn contains_key_with_options(&self, options: &ReadOptions<'_>, key: &[u8]) -> Result<bool> {
        Ok(self.read.point_lookup::<usize>(options, key)?.is_some())
    }

    /// Read using the supplied snapshot and cache policy. The returned pin owns its backing storage.
    pub fn get_pinned_with_options(
        &self,
        options: &ReadOptions<'_>,
        key: &[u8],
    ) -> Result<Option<PinnedSlice>> {
        self.read.point_lookup(options, key)
    }

    /// Read up to [`MAX_GROUPED_READ_KEYS`](crate::MAX_GROUPED_READ_KEYS) keys
    /// from one committed sequence. The result preserves input order,
    /// repeated keys and missing values. A lookup error fails the call.
    /// Each returned [`PinnedSlice`] may retain a whole block or memtable.
    pub fn get_many_pinned(&self, keys: &[&[u8]]) -> Result<Vec<Option<PinnedSlice>>> {
        self.get_many_pinned_with_options(&ReadOptions::default(), keys)
    }

    /// Read a group using the supplied snapshot and cache policy. The
    /// snapshot, when present, must belong to this DB, including for an
    /// empty group. On a usable handle an oversized group returns
    /// [`ErrorKind::InvalidArgument`](crate::ErrorKind::InvalidArgument).
    /// As with [`Self::get_pinned_with_options`], scan bounds and scan-only
    /// predicates do not filter point reads.
    pub fn get_many_pinned_with_options(
        &self,
        options: &ReadOptions<'_>,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<PinnedSlice>>> {
        self.read.grouped_point_lookup(options, keys)
    }

    /// Create a forward iterator over the entire database.
    ///
    /// Scans all keys in order. No SST pruning is applied.
    /// Equivalent to `iter_with_options(&ReadOptions::default())`.
    ///
    /// Prefer `iter_with_prefix()` or `iter_with_range()` when only a subset of
    /// keys is needed — both can skip irrelevant SST files.
    pub fn iter(&self) -> Result<DBIterator> {
        self.iter_with_options(&ReadOptions::default())
    }

    /// Create a forward iterator over the entire database with options.
    ///
    /// Uses streaming `TableIterator`s for SST files (O(1 block) memory per SST).
    /// Use `ReadOptions::iterate_lower_bound` / `iterate_upper_bound` to restrict
    /// the scan and prune SST point sources outside those bounds. Range
    /// tombstones that can cover in-range keys are retained independently.
    pub fn iter_with_options(&self, options: &ReadOptions) -> Result<DBIterator> {
        self.iter_with_range(options, None, None)
    }

    /// Create a forward iterator for an arbitrary key range `[lower_bound, upper_bound)`.
    ///
    /// Each explicit bound is intersected with the bounds in [`ReadOptions`].
    /// `None` leaves that side open. The scan prunes point sources outside the
    /// resulting range and accounts separately for overlapping range tombstones.
    /// For keys that share a prefix, [`Self::iter_with_prefix`] can also use
    /// the SST's prefix Bloom filter.
    pub fn iter_with_range(
        &self,
        options: &ReadOptions,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> Result<DBIterator> {
        self.read
            .scan(options, lower_bound, upper_bound, None, None)
    }

    /// Create a prefix-bounded iterator with full options support.
    ///
    /// Iterates over all keys that start with `prefix` in order.
    ///
    /// The prefix interval is intersected with the bounds in [`ReadOptions`].
    /// When an SST has a prefix Bloom filter and `prefix.len() >= prefix_len`,
    /// a negative probe skips that file's point entries. The scan still checks
    /// range deletions that can cover keys in the prefix.
    pub fn iter_with_prefix(&self, prefix: &[u8], options: &ReadOptions) -> Result<DBIterator> {
        let upper = types::prefix_successor(prefix);
        self.read
            .scan(options, Some(prefix), upper.as_deref(), Some(prefix), None)
    }

    /// Create a forward iterator that merges uncommitted batch writes with the
    /// current DB state. The iterator is a snapshot read at the committed
    /// sequence captured here. Batch entries use higher sequence numbers
    /// in operation order. Later commits in the active memtable stay hidden,
    /// including commits with the same sequence as an overlay entry.
    pub fn iter_with_batch(&self, batch: &WriteBatchWithIndex) -> Result<DBIterator> {
        batch.check_valid().ctx()?;
        self.read.scan(
            &ReadOptions::default(),
            None,
            None,
            None,
            (!batch.is_empty()).then_some(batch),
        )
    }

    pub(crate) fn snapshot_seq(&self) -> SequenceNumber {
        // Register the snapshot under the DB lock so that its sequence number and
        // its registration are atomic with respect to compaction, which captures
        // the snapshot list *after* picking its input files under the same lock.
        // This guarantees a compaction either observes this snapshot, or picks
        // inputs whose maximum sequence is <= this snapshot's sequence (so the
        // snapshot can never need a version the compaction might garbage-collect).
        let _inner = self.inner.lock();
        let seq = self.read.current_sequence();
        self.snapshot_list.acquire(seq)
    }

    /// Acquire a snapshot with RAII guard. The snapshot is automatically released
    /// when the `Snapshot` is dropped — no manual `release_snapshot()` needed.
    pub fn snapshot(&self) -> Snapshot<'_> {
        Snapshot::register(self)
    }

    /// Register a snapshot whose clones can travel between threads and outlive this handle.
    /// Supply its read options to this database for reads at the registered sequence.
    pub fn arc_snapshot(&self) -> ArcSnapshot {
        ArcSnapshot::register(self)
    }

    /// Release a previously acquired snapshot so compaction can reclaim its data.
    /// Called by `Snapshot::drop`.
    pub(crate) fn release_snapshot(&self, seq: SequenceNumber) {
        self.snapshot_list.release(seq);
        if self.dead_key_sweep.retry_after_snapshot_release() {
            self.read.signal_compaction();
        }
    }

    /// The path supplied by the caller at open.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Per-DB L0 metrics for the opt-in benchmark build.
    #[cfg(feature = "bench-metrics")]
    #[doc(hidden)]
    pub fn l0_compaction_profile(&self) -> crate::L0CompactionProfile {
        *self.read.stats.l0_profile.lock()
    }

    /// Get a database property.
    ///
    /// Supported properties:
    /// - `"num-files-at-level{N}"` — number of SST files at level N
    /// - `"total-sst-size"` — total size of all SST files in bytes
    /// - `"block-cache-usage"` — approximate block cache entry count.
    ///   When `DbOptions::block_cache` attaches this DB to a shared
    ///   [`BlockCachePool`](crate::BlockCachePool), the count is the
    ///   total across both pool stores for all members plus this DB's own pins.
    /// - `"compaction-pending"` — "1" if compaction is needed, "0" otherwise
    /// - `"stats.bytes_written"` — total user bytes written
    /// - `"stats.bytes_read"` — key and value bytes returned by point
    ///   lookups that find a visible value, including existence checks
    /// - `"stats.compactions_completed"` — number of compactions completed
    /// - `"stats.compaction_bytes_written"` — total bytes written during compaction
    /// - `"stats.flushes_completed"` — number of memtable flushes
    /// - `"stats.block_cache_hits"` — block cache hit count
    /// - `"stats.block_cache_misses"` — block cache miss count
    /// - `"stats.cache_hit_rate"` — block cache hit rate (0.0 to 1.0)
    pub fn get_property(&self, name: &str) -> Option<String> {
        let inner = self.inner.lock();

        if let Some(level_str) = name.strip_prefix("num-files-at-level") {
            if let Ok(level) = level_str.parse::<usize>() {
                if let Some(ref props) = inner.closed_properties {
                    return props.files_per_level.get(level).map(usize::to_string);
                }
                let version = inner.versions.current();
                if level < version.num_levels {
                    return Some(version.level_files(level).len().to_string());
                }
            }
            return None;
        }

        match name {
            "total-sst-size" => {
                let total = inner.closed_properties.as_ref().map_or_else(
                    || inner.versions.current().total_sst_size(),
                    |props| props.total_sst_size,
                );
                Some(total.to_string())
            }
            "block-cache-usage" => Some(self.block_cache.entry_count().to_string()),
            "compaction-pending" => {
                let needed = inner.closed_properties.as_ref().map_or_else(
                    || LeveledCompaction::is_pending(&inner.versions.current(), &self.options),
                    |props| props.compaction_pending,
                );
                Some(if needed { "1" } else { "0" }.to_string())
            }
            "stats.bytes_written" => Some(
                self.read
                    .stats
                    .bytes_written
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.bytes_read" => Some(
                self.read
                    .stats
                    .bytes_read
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.compactions_completed" => Some(
                self.read
                    .stats
                    .compactions_completed
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.compaction_bytes_written" => Some(
                self.read
                    .stats
                    .compaction_bytes_written
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.flushes_completed" => Some(
                self.read
                    .stats
                    .flushes_completed
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.block_cache_hits" => Some(
                self.read
                    .stats
                    .block_cache_hits
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.block_cache_misses" => Some(
                self.read
                    .stats
                    .block_cache_misses
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.cache_hit_rate" => {
                let hits = self.read.stats.block_cache_hits.load(Ordering::Relaxed) as f64;
                let misses = self.read.stats.block_cache_misses.load(Ordering::Relaxed) as f64;
                let total = hits + misses;
                if total > 0.0 {
                    Some(format!("{:.4}", hits / total))
                } else {
                    Some("0.0000".to_string())
                }
            }
            _ => None,
        }
    }

    /// Force flush the active MemTable to SST.
    ///
    /// Durability is achieved once the memtable's SSTs are installed and the
    /// old WAL is retired. The post-flush L0 drain that may follow is
    /// best-effort backpressure relief: if it fails without leaving the
    /// engine in a fail-stop state (no background error, MANIFEST not
    /// poisoned), the failure is logged and `Ok` is returned — nothing from
    /// the rejected compaction was persisted or applied, and the next flush
    /// tick / background compaction retries it from a fresh pick. Persistent
    /// compaction trouble still surfaces loudly through the write path's
    /// L0 stop trigger and the background thread's fail-stop policy.
    pub fn flush(&self) -> Result<()> {
        self.check_writable().ctx()?;
        let _maintenance = self.begin_maintenance()?;
        self.flush_generation().ctx()?;
        if self.l0_file_count.load(Ordering::Relaxed) >= self.options.l0_compaction_trigger
            && let Err(e) = self.drain_l0(false)
        {
            if let Some(fatal) = self.read.fail_stop_error() {
                return Err(fatal);
            }
            tracing::warn!("post-flush L0 drain failed (will retry): {}", e);
        }
        // Off the writer-serializing lock: harvest dead-key registrations
        // that compactions since the previous flush have fully applied.
        self.prune_settled_dead_keys(Some(DEAD_KEY_PRUNE_PER_FLUSH), None, None);
        Ok(())
    }

    /// Run compaction if needed: drains L0, then force-merges every level
    /// (L1..Ln) down to as few files as possible — a full, deliberate
    /// compaction of the whole database, similar in spirit to calling
    /// `compact_range(None, None)`. Waits for any in-flight background
    /// compaction to settle first, so entries it filtered before this call
    /// are still rewritten by this pass. For light, incremental L0 relief
    /// only (e.g. from application code reacting to write throttling),
    /// prefer relying on background compaction instead of calling this
    /// directly.
    pub fn compact(&self) -> Result<()> {
        self.check_writable().ctx()?;
        let _maintenance = self.begin_maintenance()?;
        self.flush_queue.wait(self.flush_queue.target())?;
        self.force_compact_all().ctx()?;
        // Full pass done — every settled registration is now prunable.
        self.prune_settled_dead_keys(None, None, None);
        Ok(())
    }

    /// Compact all keys in the given range across all levels.
    /// If `begin` is None, starts from the beginning. If `end` is None, goes to the end.
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.check_writable().ctx()?;
        if begin.zip(end).is_some_and(|(b, e)| b >= e) {
            return Ok(());
        }

        let _maintenance = self.begin_maintenance()?;
        self.flush_generation().ctx()?;

        if begin.is_none() && end.is_none() {
            self.force_compact_all().ctx()?;
            self.prune_settled_dead_keys(None, None, None);
            return Ok(());
        }

        // Range-filtered compaction: compact files overlapping [begin, end),
        // including one bottommost rewrite pass at `num_levels - 1` for files
        // not already rewritten into that level during this call.
        let mut settled_bottom: HashSet<u64> = HashSet::new();
        loop {
            let pick = {
                let inner = self.inner.lock();
                let version = inner.versions.current();
                match LeveledCompaction::pick_compaction_for_range(
                    &version,
                    begin,
                    end,
                    &settled_bottom,
                ) {
                    Some(task) => {
                        let l0_inputs: Vec<u64> = if task.level == 0 {
                            task.input_files_level
                                .iter()
                                .map(|f| f.meta.number)
                                .collect()
                        } else {
                            Vec::new()
                        };
                        let file_numbers = inner.versions.file_numbers.clone();
                        let all_inputs: Vec<_> = task
                            .input_files_level
                            .iter()
                            .chain(task.input_files_next.iter())
                            .cloned()
                            .collect();
                        let is_bottom = LeveledCompaction::is_bottommost_level(
                            &version,
                            task.level,
                            self.options.num_levels,
                            &all_inputs,
                        );
                        // Capture the snapshot list under the DB lock, consistent
                        // with the inputs just picked, so no concurrently-acquired
                        // snapshot can be missed.
                        let active_snaps = self.snapshot_list.as_sorted_vec();
                        // The flush worker also holds new L0 files claimed until its
                        // immutable generation retires. Never compact a file that is
                        // still shadowed by that memtable.
                        let claimed_inputs: Vec<_> =
                            all_inputs.iter().map(|tf| tf.meta.number).collect();
                        let mut claimed = self.compacting_files.lock();
                        if claimed_inputs.iter().any(|n| claimed.contains(n)) {
                            drop(claimed);
                            drop(inner);
                            self.compacting_files.wait_until_empty();
                            continue;
                        }
                        claimed.extend(claimed_inputs.iter().copied());
                        Some((
                            task,
                            file_numbers,
                            l0_inputs,
                            is_bottom,
                            active_snaps,
                            claimed_inputs,
                        ))
                    }
                    None => None,
                }
            };

            let Some((task, file_numbers, l0_inputs, is_bottom, active_snaps, claimed)) = pick
            else {
                break;
            };
            let _claim = CompactionClaim {
                claims: Arc::clone(&self.compacting_files),
                numbers: claimed,
            };

            let ctx = CompactionContext {
                db_path: &self.storage_path,
                dir_sync: &self.dir_sync,
                manifest_poisoned: &self.read.manifest_poisoned,
                options: &self.options,
                builds: &self.builds,
                rate_limiter: Some(&self.rate_limiter),
                stats: Some(&self.read.stats),
                active_snapshots: &active_snaps,
            };
            let output =
                LeveledCompaction::execute_compaction_io(&ctx, &task, &file_numbers, is_bottom)
                    .ctx()?;

            // Pre-warm the table cache for the new output files while
            // unlocked (see the background compaction loop for rationale).
            self.table_cache
                .prewarm(output.edit.new_files.iter().map(|(_, meta)| meta.number));

            let rewritten_bottom_outputs: Vec<u64> = if is_bottom
                && task.target_level == self.options.num_levels - 1
                && !output.input_file_numbers.is_empty()
            {
                output
                    .edit
                    .new_files
                    .iter()
                    .map(|(_, meta)| meta.number)
                    .collect()
            } else {
                Vec::new()
            };

            let cleanup = {
                let mut inner = self.inner.lock();
                let cleanup = LeveledCompaction::install_compaction(
                    output,
                    &mut inner.versions,
                    Some(&self.table_cache),
                    Some(&self.block_cache),
                    &self.storage_path,
                    Some(&self.read.stats),
                    !self.snapshot_list.is_empty(),
                )
                .ctx()?;
                if cleanup.installed {
                    settled_bottom.extend(rewritten_bottom_outputs);
                }
                // Unpin L0 inputs that actually left L0 — a trivial move
                // relabels the same physical file to L1 (not in
                // files_to_delete), while a discarded (stale) install leaves
                // the file in L0 and its first-block pin must stay.
                if !l0_inputs.is_empty() {
                    let current = inner.versions.current();
                    for num in &l0_inputs {
                        if !current
                            .level_files(0)
                            .iter()
                            .any(|tf| tf.meta.number == *num)
                        {
                            self.block_cache.unpin_file(*num);
                        }
                    }
                }
                // Publish the new SuperVersion after every iteration (not
                // just once after the whole range-compaction cascade) so
                // this iteration's already-unlinked input files' Arc<TableReader>
                // references — and their disk space — are released promptly,
                // mirroring drain_l0's per-iteration refresh. Deferring this
                // to a single post-loop refresh would keep every superseded
                // file across the whole multi-level cascade open until the
                // entire compact_range call finished, transiently doubling
                // disk usage for an API whose purpose is reclaiming space.
                self.l0_file_count
                    .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
                self.install_super_version(&inner);
                cleanup
            };
            // Sync manifest + delete old SSTs outside the main lock
            if let Err(e) = {
                let manifest_handle = self.inner.lock().versions.manifest_sync_handle();
                confirm_manifest_durable(&manifest_handle, &self.read.manifest_poisoned)
            } {
                // The edit is applied in memory but its durability could
                // not be confirmed — the same fail-stop condition as
                // drain_l0 and the background threads. Poison (if not already)
                // is enforced inside confirm_manifest_durable; record the
                // background error so every entry point sees a fatal engine
                // state, not a retryable hiccup.
                self.set_bg_error(format!("compact_range manifest sync failed: {}", e));
                return Err(e).ctx();
            }
            LeveledCompaction::run_post_compaction_cleanup(&cleanup, &self.storage_path);
        }

        // A range-scoped pass can only newly settle registrations inside
        // [begin, end): only those are probed. Out-of-range settled keys are
        // harvested by later capped flush passes or a full compact().
        self.prune_settled_dead_keys(None, begin, end);
        Ok(())
    }

    /// Register a key for lazy deletion. The key will be silently dropped
    /// during the next compaction that encounters it, without writing a
    /// tombstone. This avoids write amplification for bulk cleanup.
    ///
    /// Once the number of accumulated dead keys reaches
    /// `lazy_delete_compaction_threshold` (default: `0`, i.e. disabled —
    /// this is an opt-in feature), every newly registered unique key queues
    /// a background sweep: every populated level is force-rewritten through
    /// the compaction filter (same behavior as
    /// [`lazy_delete_batch`](Self::lazy_delete_batch)), so the keys are
    /// physically removed even when the store has no organic compaction
    /// debt. Removal is subject to the usual bottommost-safety rule: a
    /// version shadowing an older version at a deeper level is kept until
    /// later compactions reach that data, exactly as with
    /// [`compact`](Self::compact).
    ///
    /// Each level's sweep claims its input files, releases the DB mutex
    /// during SST construction, then verifies the inputs before install.
    /// Snapshots taken during a filter rewrite retain their original values.
    ///
    /// **Note:** The key remains readable via `get()` until compaction
    /// physically removes it. Use regular `delete()` if immediate
    /// invisibility is required.
    ///
    /// **Warning:** Dead keys are held in memory only and are **not**
    /// persisted to WAL or SST. If the process crashes or restarts
    /// before compaction removes them, the registrations are lost and
    /// the keys will remain in the database permanently unless
    /// re-registered.
    ///
    /// A registration is pruned automatically once no visible version of
    /// the key remains (checked after flushes and after explicit
    /// [`compact`](Self::compact) / [`compact_range`](Self::compact_range)
    /// calls), so a fully applied lazy deletion stops consuming memory. A
    /// key written *after* its registration was pruned is a normal live
    /// key; a key re-written *while* its registration is still active is
    /// removed by a later compaction like any other registered key — do
    /// not reuse a key until its lazy deletion has completed or
    /// [`clear_dead_keys`](Self::clear_dead_keys) has been called.
    ///
    /// This method is a no-op when the DB was opened read-only.
    pub fn lazy_delete(&self, key: &[u8]) {
        if self.read.read_only {
            return;
        }
        self.register_dead_key(key.to_vec());
    }

    fn register_dead_key(&self, key: Vec<u8>) {
        let threshold = self.options.lazy_delete_compaction_threshold;
        let mut set = self.dead_keys.write();
        let inserted = set.insert(key);
        let len = set.len();
        drop(set);
        if threshold > 0 && len >= threshold && inserted {
            self.request_dead_key_sweep();
        }
    }

    /// Batch version of [`lazy_delete`](Self::lazy_delete). The input iterator
    /// is consumed before the batch is registered, so it can query this DB.
    ///
    /// Dead keys are **not** persisted — see [`lazy_delete`](Self::lazy_delete)
    /// for durability caveats.
    ///
    /// At or above `lazy_delete_compaction_threshold` (default: `0`, i.e.
    /// disabled — opt-in only), a batch containing any newly registered key
    /// queues a background sweep — see [`lazy_delete`](Self::lazy_delete) for
    /// the scheduling and snapshot semantics.
    /// This method is a no-op when the DB was opened read-only.
    pub fn lazy_delete_batch(&self, keys: impl IntoIterator<Item = impl AsRef<[u8]>>) {
        if self.read.read_only {
            return;
        }
        let mut keys = keys.into_iter();
        let Some(first) = keys.next().map(|key| key.as_ref().to_vec()) else {
            return;
        };
        let Some(second) = keys.next() else {
            drop(keys);
            self.register_dead_key(first);
            return;
        };

        // Reuse an empty registry's allocation. Otherwise stage short batches
        // contiguously and deduplicate excess input to bound duplicate storage.
        let mut budget = 32 * 1024usize;
        let mut distinct = {
            let mut set = self.dead_keys.write();
            if set.is_empty() {
                budget = 0;
                mem::take(&mut *set)
            } else {
                HashSet::new()
            }
        };
        let mut bytes = Vec::new();
        let mut ends = Vec::new();
        let mut stage = |key: &[u8]| {
            let cost = key.len() + size_of::<usize>();
            if cost <= budget {
                budget -= cost;
                bytes.extend_from_slice(key);
                ends.push(bytes.len());
            } else {
                distinct.insert(key.to_vec());
            }
        };
        stage(&first);
        for item in std::iter::once(second).chain(keys) {
            stage(item.as_ref());
        }

        // Publish once, after every caller-controlled callback and destructor.
        let threshold = self.options.lazy_delete_compaction_threshold;
        let mut set = self.dead_keys.write();
        let before = set.len();
        if distinct.len() > before {
            mem::swap(&mut *set, &mut distinct);
        }
        for key in distinct {
            set.insert(key);
        }
        let mut changed = set.len() > before;
        let mut start = 0;
        for end in ends {
            let key = &bytes[start..end];
            // Existing batches need no key copies. After the first addition,
            // insert directly so new keys need only one hash probe each.
            if changed || !set.contains(key) {
                set.insert(key.to_vec());
                changed = true;
            }
            start = end;
        }
        let len = set.len();
        drop(set);
        if threshold > 0 && len >= threshold && changed {
            self.request_dead_key_sweep();
        }
    }

    /// Arm the background dead-key sweep and wake the compaction thread.
    /// The sweep force-rewrites every populated level through the
    /// compaction filter — a plain `signal_compaction()` is not enough,
    /// because the background thread's debt-driven picks find nothing to
    /// do on a settled store and the registered keys would never be
    /// physically removed.
    fn request_dead_key_sweep(&self) {
        self.dead_key_sweep.queue();
        self.read.signal_compaction();
    }

    /// Remove dead-key registrations that have been fully applied.
    ///
    /// Once no visible version of a registered key remains, compaction can
    /// never encounter that key again: keeping the registration only wastes
    /// memory and keeps the internal lazy-delete filter non-noop, which
    /// permanently disables the trivial-move optimization. A key that is
    /// merely shadowed by a newer point/range tombstone is also settled —
    /// the tombstone itself guarantees the remnants are dropped by normal
    /// compaction GC.
    ///
    /// `max_checks` bounds how many registrations are probed (flush-driven
    /// passes are capped so no caller stalls on an unbounded scan); `None`
    /// probes every candidate. A rotating cursor makes successive capped
    /// passes cover the whole set. `lower`/`upper` restrict the candidates
    /// to the half-open window `[lower, upper)` — a range-scoped compaction
    /// can only newly settle keys inside its own range, so probing the rest
    /// would be wasted reads. Lookup errors (e.g. fail-stop) conservatively
    /// keep the registration.
    ///
    /// The bulk absence probe runs unlocked so a large candidate set never
    /// stalls writers, then the settled subset is re-verified and removed
    /// under `write_queue` so a concurrent write cannot land in the gap
    /// between "observed absent" and "registration removed". That locked
    /// phase is chunked (`DEAD_KEY_RECONFIRM_CHUNK`) because only each key's
    /// own check-and-remove pair must be atomic — see `write_queue`'s use
    /// below.
    fn prune_settled_dead_keys(
        &self,
        max_checks: Option<usize>,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) {
        let in_window = |key: &Vec<u8>| {
            lower.is_none_or(|lo| key.as_slice() >= lo)
                && upper.is_none_or(|hi| key.as_slice() < hi)
        };
        let candidates: Vec<Vec<u8>> = {
            let set = self.dead_keys.read();
            if set.is_empty() {
                return;
            }
            match max_checks {
                Some(limit) if limit < set.len() => {
                    let start = self
                        .dead_key_prune_cursor
                        .fetch_add(limit, Ordering::Relaxed)
                        % set.len();
                    // `take(set.len())` bounds the rotation to one full lap:
                    // without it, a window matching no key would make
                    // `cycle` + `filter` spin forever.
                    set.iter()
                        .cycle()
                        .skip(start)
                        .take(set.len())
                        .filter(|key| in_window(key))
                        .take(limit)
                        .cloned()
                        .collect()
                }
                _ => set.iter().filter(|key| in_window(key)).cloned().collect(),
            }
        };
        // One-shot probes: don't let them evict hot blocks from the cache.
        let read_options = ReadOptions {
            fill_cache: false,
            ..ReadOptions::default()
        };
        let settled: Vec<Vec<u8>> = candidates
            .into_iter()
            .filter(|key| matches!(self.get_with_options(&read_options, key), Ok(None)))
            .collect();
        if settled.is_empty() {
            return;
        }

        // Test-only: let a regression test deterministically run a
        // concurrent write right here — after the absence probe above but
        // before the re-verify-and-remove step below.
        #[cfg(test)]
        if let Some(hook) = PRUNE_SETTLED_RACE_HOOK.with(|h| h.borrow_mut().take()) {
            hook();
        }

        // The probe above is a lock-free, unlocked scan so a large candidate
        // set never stalls writers — but that leaves a check-then-act gap: a
        // concurrent put/write_batch can commit a new version of a key we
        // just saw absent before we remove its registration below, and the
        // removal would then permanently hide a rewrite that raced the
        // pruning decision from any future compaction filter. Re-verify each
        // key while holding `write_queue` after any in-flight leader
        // finishes: no writer can even enqueue past that lock (`write()`
        // takes the same mutex before joining or becoming leader), so nothing
        // can commit between a key's second check and its removal — the same
        // serialization `compact`/`compact_range`/`close` already rely on.
        //
        // Only each key's own re-verify-and-remove pair has to be atomic with
        // respect to writes, so the pass is chunked and the lock is released
        // between chunks. Holding it across the whole `settled` list would
        // make every writer wait out that many lookups; each one is a real
        // read (memtables + every L0 file + one file per lower level, with
        // Bloom unable to help once a rewritten key is present), so a large
        // registration set turns an unlocked scan into a proportional write
        // stall.
        for chunk in settled.chunks(DEAD_KEY_RECONFIRM_CHUNK) {
            #[cfg(test)]
            PRUNE_SETTLED_LOCK_COUNT.with(|c| c.set(c.get() + 1));
            let _barrier = self.acquire_write_barrier();
            let reconfirmed: Vec<&Vec<u8>> = chunk
                .iter()
                .filter(|key| matches!(self.get_with_options(&read_options, key), Ok(None)))
                .collect();
            if reconfirmed.is_empty() {
                continue;
            }
            let mut set = self.dead_keys.write();
            for key in reconfirmed {
                set.remove(key);
            }
        }
    }

    /// Returns the current number of keys registered for lazy deletion.
    pub fn dead_key_count(&self) -> usize {
        self.dead_keys.read().len()
    }

    /// Clear all dead keys to reclaim memory used by the dead-keys set.
    ///
    /// Fully applied registrations are already pruned automatically (see
    /// [`lazy_delete`](Self::lazy_delete)); this exists to abandon
    /// registrations that are still pending.
    ///
    /// **Caution:** Only call this after a full compaction
    /// ([`compact_range(None, None)`](Self::compact_range)) has
    /// finished. Any dead keys that have not yet been visited by
    /// compaction will be silently abandoned — those keys will remain
    /// in the database and will not be removed unless re-registered.
    pub fn clear_dead_keys(&self) {
        self.dead_keys.write().clear();
    }

    /// Close the database.
    ///
    /// Resources are released even when this returns a flush, sync, or
    /// previously recorded fail-stop error. A compaction filter or block
    /// property collector closing its own store receives `InvalidArgument`
    /// because shutdown must wait for the callback to return.
    pub fn close(&self) -> Result<()> {
        if self.builds.contains_current_thread() {
            return Err(Error::invalid_argument(
                "cannot close a store from its own SST build callback",
            ));
        }
        // All close callers wait for worker shutdown and the directory unlock.
        // Workers never acquire this guard, including during their final install.
        let mut directory_lock = self.lock_file.lock();
        if self.read.closed.swap(true, Ordering::AcqRel) {
            return self.read.fail_stop_error().map_or(Ok(()), Err);
        }
        {
            let mut maintenance = self.maintenance.lock();
            maintenance.closing = true;
            while maintenance.active != 0 {
                self.maintenance_cv.wait(&mut maintenance);
            }
        }
        drop(self.acquire_write_barrier());
        let mut first_error = self.read.fail_stop_error();
        if first_error.is_none() && !self.read.read_only {
            let result = self
                .submit_active()
                .and_then(|target| self.flush_queue.wait(target));
            first_error = result.err();
        }
        self.stop_background_and_detach_cache();
        self.flush_queue.cleanup_standby_wal(&self.storage_path);
        let mut inner = self.inner.lock();
        let version = inner.versions.current();
        inner.closed_properties = Some(ClosedProperties {
            files_per_level: version.files.iter().map(|level| level.len()).collect(),
            total_sst_size: version.total_sst_size(),
            compaction_pending: LeveledCompaction::is_pending(&version, &self.options),
        });
        drop(version);
        inner.wal_writer.take();
        inner.versions.release_files();
        inner.active_memtable = Arc::new(MemTable::new());
        inner.immutable_memtables.clear();
        self.install_super_version(&inner);
        drop(inner);
        self.table_cache.close();
        directory_lock.take();

        if first_error.is_none() {
            first_error = self.read.fail_stop_error();
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    // -- Internal --

    /// Acquire `write_queue` and wait until no group-commit leader is active.
    pub(super) fn acquire_write_barrier(&self) -> WriteBarrier<'_> {
        let mut wq = self.write_queue.lock();
        if wq.leader_active {
            wq.waiting_barriers += 1;
            while wq.leader_active {
                self.write_cv.wait(&mut wq);
            }
            wq.waiting_barriers -= 1;
            if wq.waiting_barriers > 0 {
                self.write_cv.notify_all();
            }
        }
        WriteBarrier {
            db: self,
            wq: Some(wq),
        }
    }

    /// Notify `wq.queue.front()` that it may claim leadership once awake, while
    /// leaving `leader_active` false so an already-running writer can drain the
    /// queue immediately without waiting for an OS scheduler wakeup.
    fn wake_queue_head(wq: &WriteQueueState) -> Option<thread::Thread> {
        let &head = wq.queue.front()?;
        // SAFETY: `head` is still in `wq.queue` (`STATE_IN_GROUP` and `STATE_DONE`
        // are unset while `wq` is held), so its stack frame is live and `waiter`
        // is immutable. Projecting fields avoids forming `&WriteRequest`.
        unsafe {
            let waiter = (*head).waiter.clone();
            let prev = (*head).state.fetch_or(STATE_WAKE, Ordering::AcqRel);
            if (prev & STATE_PARKED) != 0 {
                waiter
            } else {
                None
            }
        }
    }

    /// Release `leader_active` and wake waiting barriers or notify the head queued writer.
    pub(super) fn release_write_leadership(&self) {
        let wake = {
            let mut wq = self.write_queue.lock();
            wq.leader_active = false;
            if wq.waiting_barriers != 0 {
                self.write_cv.notify_all();
                None
            } else {
                Self::wake_queue_head(&wq)
            }
        };
        if let Some(t) = wake {
            t.unpark();
        }
    }

    fn stop_background_and_detach_cache(&self) {
        self.flush_queue.stop();
        if let Some(worker) = self.flush_worker.lock().take() {
            let _ = worker.join();
        }
        {
            let (lock, cvar) = &*self.read.compaction_notify;
            let _guard = lock.lock();
            self.compaction_shutdown.store(true, Ordering::Release);
            cvar.notify_all();
        }
        for handle in self.compaction_handles.lock().drain(..) {
            let _ = handle.join();
        }
        self.block_cache.detach();
    }

    /// Refresh the SuperVersion from current inner state.
    /// Called after memtable freeze, flush, or compaction.
    fn install_super_version(&self, inner: &DBInner) {
        refresh_super_version(&self.read.super_version, inner);
    }

    /// Record a background error, setting the fast-path flag and the detailed message.
    /// All subsequent `check_usable()` calls will return this error.
    fn set_bg_error(&self, msg: String) {
        *self.read.bg_error.lock() = Some(msg.clone());
        self.read.has_bg_error.store(true, Ordering::Release);
        self.flush_queue.fail(Error::background(msg));
    }

    fn check_writable(&self) -> Result<()> {
        if self.builds.contains_current_thread() {
            return Err(Error::invalid_argument(
                "cannot mutate a store from its own SST build callback",
            ));
        }
        self.read.check_usable()?;
        if self.read.read_only {
            return Err(Error::read_only());
        }
        Ok(())
    }

    /// Apply write backpressure based on L0 file count.
    fn maybe_throttle_writes(&self) -> Result<()> {
        // Fast path: check cached L0 count without locking inner.
        let l0_count = self.l0_file_count.load(Ordering::Relaxed);

        if l0_count >= self.options.l0_stop_trigger {
            crate::profile::time_scope!(l0_stop_ns, l0_stops);
            // Hold write_queue across the entire stop-trigger drain so close
            // cannot take the barrier, release LOCK/caches, and let this path
            // install SST/MANIFEST afterwards. Keep the slowdown sleep outside
            // the mutex: it does not mutate durable state.
            let _barrier = self.acquire_write_barrier();
            self.check_writable().ctx()?;
            let still_over = {
                let inner = self.inner.lock();
                inner.versions.current().l0_file_count() >= self.options.l0_stop_trigger
            };
            if still_over {
                #[cfg(feature = "bench-metrics")]
                crate::profile::record(|p| p.l0_drains += 1);
                // Slow path: drain L0, not the whole database, and do not hold
                // db_mutex across the merge I/O. write_queue stays held for
                // close serialization. drain_l0(false) returns Ok while another
                // compaction holds the L0 claim; admitting the write then lets
                // later flushes add L0 files for as long as that claim is live.
                // Wait until L0 is actually drained. If compaction fails,
                // admitting the write would silently disable the stall —
                // fail-stop instead, matching the background thread.
                if let Err(e) = self.drain_l0(true) {
                    let msg = format!("inline L0 compaction failed: {}", e);
                    self.set_bg_error(msg.clone());
                    return Err(Error::background(msg));
                }
            }
            self.l0_file_count.store(
                self.inner.lock().versions.current().l0_file_count(),
                Ordering::Relaxed,
            );
            drop(_barrier);
        } else if l0_count >= self.options.l0_slowdown_trigger {
            // Ensure the backlog is actually being worked on. The slowdown
            // state can be entered without any prior signal (e.g. an L0
            // backlog inherited from a previous process at open, before the
            // startup kick has drained it), and signalling is idempotent.
            self.read.signal_compaction();
            // Progressive delay: more L0 files → longer sleep
            let delay_us = (l0_count - self.options.l0_slowdown_trigger + 1) as u64 * 1000;
            crate::profile::timed!(
                l0_slowdown_ns,
                l0_slowdowns,
                thread::sleep(Duration::from_micros(delay_us))
            );
            // Refresh the cached count so a compaction finishing while we
            // slept lifts the slowdown for the next writes immediately —
            // without this, small workloads that never flush would keep
            // paying the full penalty long after L0 has been drained.
            self.l0_file_count.store(
                self.inner.lock().versions.current().l0_file_count(),
                Ordering::Relaxed,
            );
        }
        Ok(())
    }

    fn write_batch_inner(&self, batch: WriteBatch, write_options: &WriteOptions) -> Result<()> {
        batch.check_valid().ctx()?;
        if batch.is_empty() {
            return Ok(());
        }

        self.flush_queue.check_capacity(
            self.options.max_immutable_memtables,
            write_options.no_slowdown,
        )?;
        if write_options.no_slowdown {
            // A no_slowdown write fails fast whenever a blocking path would
            // engage. Open-time validation guarantees
            // l0_slowdown_trigger <= l0_stop_trigger, so the slowdown
            // trigger is the first threshold at which either blocking path
            // (the slowdown delay or the stop-trigger drain) can activate.
            let l0_count = self.l0_file_count.load(Ordering::Relaxed);
            if l0_count >= self.options.l0_slowdown_trigger {
                // An earlier wake may have found an overlapping maintenance claim.
                // Leave eligible L0 work queued even when all writers are no_slowdown.
                if l0_count >= self.options.l0_compaction_trigger {
                    self.read.signal_compaction();
                }
                return Err(Error::busy(
                    "L0 file count reached the write slowdown threshold",
                ));
            }
        } else {
            self.maybe_throttle_writes().ctx()?;
        }

        self.enqueue_write(WriteRequest {
            batch,
            kind: WriteRequestKind::Batch,
            no_slowdown: write_options.no_slowdown,
            durability: write_options.durability,
            waiter: None,
            result: None,
            state: AtomicU8::new(REQ_QUEUED),
        })
    }

    /// Make earlier WAL-enabled writes durable without adding a record or advancing MVCC.
    pub fn sync_wal(&self) -> Result<()> {
        self.check_writable()?;
        let target_seq = self.read.current_sequence();
        if self.synced_wal_seq.load(Ordering::Acquire) >= target_seq {
            return Ok(());
        }
        self.enqueue_write(WriteRequest {
            kind: WriteRequestKind::WalFence { target_seq },
            no_slowdown: false,
            batch: WriteBatch::new(),
            durability: DurabilityMode::DisableWal,
            waiter: None,
            result: None,
            state: AtomicU8::new(REQ_QUEUED),
        })
    }

    fn complete_write_group(
        batch_group: &[*mut WriteRequest],
        self_ptr: *mut WriteRequest,
        default_result: &Result<()>,
    ) {
        for &rp in batch_group {
            // SAFETY: all request pointers were drained exclusively by this
            // leader and stay live on their owners' stacks until `STATE_DONE`
            // is published via `swap`. Raw field projections avoid forming
            // `&WriteRequest` / `&mut WriteRequest`, and `rp` is never touched
            // again after swapping `STATE_DONE`.
            unsafe {
                if (*rp).result.is_none() {
                    (*rp).result = Some(default_result.clone());
                }
                let waiter = if rp == self_ptr {
                    None
                } else {
                    (*rp).waiter.take()
                };
                let prev = (*rp).state.swap(STATE_DONE, Ordering::AcqRel);
                if (prev & STATE_PARKED) != 0
                    && let Some(t) = waiter
                {
                    t.unpark();
                }
            }
        }
    }

    fn enqueue_write(&self, mut req: WriteRequest) -> Result<()> {
        let spin_limit: u32 = if req.durability == DurabilityMode::WalSync
            || matches!(req.kind, WriteRequestKind::WalFence { .. })
        {
            0
        } else {
            384
        };
        let payload_len = req.batch.payload.len();
        let req_ptr = &raw mut req;

        // Fast path: become leader and drain immediately in one lock acquisition
        // when no leader or barrier is active and `drain_group` will admit `req_ptr`
        // (either the queue is empty so `req_ptr` is index 0, or the entire pending
        // queue plus `req_ptr` fits within the group byte cap). Otherwise register
        // our `Thread` handle, enqueue, and wait without re-locking on completion.
        let mut wake_head: Option<thread::Thread> = None;
        let initial_group = {
            let mut wq = self.write_queue.lock();
            if !wq.leader_active
                && wq.waiting_barriers == 0
                && wq.can_drain_with(payload_len, self.options.write_buffer_size)
            {
                wq.leader_active = true;
                wq.queue.push_back(req_ptr);
                Some(wq.drain_group(self.options.write_buffer_size))
            } else {
                // SAFETY: `req` has not been shared yet; we hold `wq` before pushing `req_ptr`.
                unsafe {
                    (*req_ptr).waiter = Some(thread::current());
                }
                wq.queue.push_back(req_ptr);
                if !wq.leader_active && wq.waiting_barriers == 0 {
                    wake_head = Self::wake_queue_head(&wq);
                }
                None
            }
        };
        if let Some(t) = wake_head {
            t.unpark();
        }

        let batch_group = match initial_group {
            Some(group) => group,
            None => {
                #[cfg(feature = "bench-metrics")]
                crate::profile::record(|p| p.queue_waits += 1);
                let mut completed = false;
                let mut promoted_group: Option<WriteGroup> = None;
                let mut spins = 0u32;
                loop {
                    // SAFETY: `req_ptr` points to our own stack `req`;
                    // projecting `state` avoids forming `&req` while queued.
                    let s = unsafe { (*req_ptr).state.load(Ordering::Acquire) };
                    if (s & STATE_DONE) != 0 {
                        completed = true;
                        break;
                    }
                    if (s & STATE_WAKE) != 0 && (s & STATE_IN_GROUP) == 0 {
                        let mut wq = self.write_queue.lock();
                        // SAFETY: `req_ptr` points to our own stack `req`;
                        // projecting `state` avoids forming `&req` while queued.
                        let s_locked = unsafe { (*req_ptr).state.load(Ordering::Acquire) };
                        if (s_locked & STATE_DONE) != 0 {
                            completed = true;
                            break;
                        }
                        if (s_locked & STATE_IN_GROUP) == 0 {
                            if !wq.leader_active
                                && wq.waiting_barriers == 0
                                && wq.queue.front() == Some(&req_ptr)
                            {
                                wq.leader_active = true;
                                promoted_group =
                                    Some(wq.drain_group(self.options.write_buffer_size));
                                break;
                            }
                            // SAFETY: `req_ptr` points to our own stack `req`,
                            // and `state` is updated atomically via raw projection.
                            unsafe {
                                (*req_ptr)
                                    .state
                                    .fetch_and(!(STATE_WAKE | STATE_PARKED), Ordering::AcqRel);
                            }
                        }
                        drop(wq);
                    }
                    if spins < spin_limit {
                        spins += 1;
                        std::hint::spin_loop();
                    } else {
                        // SAFETY: `req_ptr` points to our own stack `req`, and
                        // `state` is updated atomically via raw projection.
                        let prev =
                            unsafe { (*req_ptr).state.fetch_or(STATE_PARKED, Ordering::AcqRel) };
                        if (prev & STATE_DONE) != 0 {
                            completed = true;
                            break;
                        }
                        if (prev & STATE_WAKE) != 0 && (prev & STATE_IN_GROUP) == 0 {
                            continue;
                        }
                        crate::profile::timed!(queue_wait_ns, queue_wakeups, thread::park());
                    }
                }
                if completed {
                    return req.result.take().unwrap_or(Ok(()));
                }
                promoted_group.expect("woken non-completed follower must hold promoted group")
            }
        };
        debug_assert!(batch_group.contains(&req_ptr));

        let mut followers_completed = false;
        let result = self.write_batch_group(&batch_group, req_ptr, &mut followers_completed);
        let group_flushed = matches!(result, Ok(true));

        if !followers_completed {
            let default_res = result.as_ref().map(|_| ()).map_err(Clone::clone);
            Self::complete_write_group(&batch_group, req_ptr, &default_res);
        }

        self.release_write_leadership();

        // Leadership released and every waiter woken: harvest dead-key
        // registrations settled by compactions since the previous flush.
        // Runs on the leader's own time so queued writers never pay for it.
        if group_flushed {
            self.prune_settled_dead_keys(Some(DEAD_KEY_PRUNE_PER_FLUSH), None, None);
        }

        req.result.take().unwrap_or(Ok(()))
    }

    // Snapshot registration and group publication both hold inner. At the last
    // range write covering any elementary interval, one sequence can be needed
    // per distinct snapshot plus the group's newest visible sequence. Snapshots
    // taken later all see that same latest range unless another write covers it.
    fn admit_range_stripes(&self, requests: &mut WriteGroup) {
        #[cfg(not(test))]
        let limit = MAX_RANGE_STRIPES;
        #[cfg(test)]
        let limit = self
            .snapshot_list
            .range_stripe_limit
            .load(Ordering::Relaxed);
        if self.snapshot_list.count.load(Ordering::Relaxed) < limit {
            return;
        }
        let mut full = None;
        requests.retain(|&mut ptr| {
            // SAFETY: the leader has exclusive access to result, and batch is
            // immutable until completion.
            let batch = unsafe { &(*ptr).batch };
            if batch
                .entries()
                .any(|entry| entry.value_type == ValueType::RangeDeletion)
                && *full.get_or_insert_with(|| self.snapshot_list.as_sorted_vec().len() >= limit)
            {
                // SAFETY: only the active leader writes `result` before `STATE_DONE`.
                unsafe {
                    (*ptr).result = Some(Err(Error::invalid_argument(
                        "too many distinct live snapshot sequences for a range-deletion SST leaf",
                    )))
                };
                return false;
            }
            true
        });
    }

    // SST files split between user keys. Bound the root-index footprint of
    // a single unsplittable run before assigning sequences or appending WAL.
    fn admit_same_key_runs(memtable: &MemTable, requests: &mut WriteGroup) {
        let group_bound = requests.iter().fold(0usize, |sum, &ptr| {
            // SAFETY: queued batches are immutable until completion.
            let batch = unsafe { &(*ptr).batch };
            sum.saturating_add(batch.payload.len())
                .saturating_add(batch.len().saturating_mul(40))
        });
        if memtable.max_metadata_charge().saturating_add(group_bound) <= META_BLOCK_SPLIT_THRESHOLD
        {
            return;
        }

        let mut charges = std::collections::HashMap::<&[u8], usize>::new();
        requests.retain(|&mut ptr| {
            // SAFETY: the owner's frame stays live until this group completes.
            let batch = unsafe { &(*ptr).batch };
            let suspect = |entry: &types::BatchEntry<'_>| {
                memtable
                    .metadata_charge_bound(entry.key)
                    .saturating_add(group_bound)
                    > META_BLOCK_SPLIT_THRESHOLD
            };
            for (index, entry) in batch.entries().filter(suspect).enumerate() {
                let total = charges
                    .entry(entry.key)
                    .or_insert_with(|| memtable.same_key_metadata(entry.key));
                if total.saturating_add(entry.metadata_charge()) > META_BLOCK_SPLIT_THRESHOLD {
                    // Roll back only this rejected batch so peer requests
                    // still see every previously admitted operation.
                    for applied in batch.entries().filter(suspect).take(index) {
                        *charges.get_mut(applied.key).unwrap() -= applied.metadata_charge();
                    }
                    // SAFETY: only the active leader writes `result` before `STATE_DONE`.
                    unsafe {
                        (*ptr).result = Some(Err(Error::invalid_argument(
                            "same-key metadata exceeds SST partition index budget",
                        )))
                    };
                    return false;
                }
                *total += entry.metadata_charge();
            }
            true
        });
    }

    /// Process a batch group: assign sequences, write+flush WAL, then publish to MemTable.
    /// Returns `true` when the group's writes triggered a memtable flush, so
    /// the leader can harvest settled dead-key registrations after it has
    /// woken the group's waiters.
    fn write_batch_group(
        &self,
        orig_batch_group: &[*mut WriteRequest],
        self_ptr: *mut WriteRequest,
        followers_completed: &mut bool,
    ) -> Result<bool> {
        self.check_writable()?;
        let mut group = WriteGroup::with_capacity(orig_batch_group.len());
        for &ptr in orig_batch_group {
            // SAFETY: the leader exclusively processes the queued request's fields.
            let no_slowdown =
                unsafe { matches!((*ptr).kind, WriteRequestKind::Batch) && (*ptr).no_slowdown };
            if no_slowdown
                && let Err(e) = self
                    .flush_queue
                    .check_capacity(self.options.max_immutable_memtables, true)
            {
                // SAFETY: only the active leader writes this field until completion.
                unsafe { (*ptr).result = Some(Err(e)) };
            } else {
                group.push(ptr);
            }
        }
        // SAFETY: kind is immutable while its owner waits for completion.
        if group
            .iter()
            .any(|&ptr| matches!(unsafe { (*ptr).kind }, WriteRequestKind::Batch))
        {
            self.flush_queue
                .check_capacity(self.options.max_immutable_memtables, false)?;
        }
        let mut need_sync = group.iter().any(|&ptr| {
            // SAFETY: the queued request remains live and kind is immutable.
            matches!(unsafe { (*ptr).kind }, WriteRequestKind::WalFence { target_seq } if target_seq > self.synced_wal_seq.load(Ordering::Acquire))
        });
        let mut inner = self.inner.lock();

        self.admit_range_stripes(&mut group);
        Self::admit_same_key_runs(&inner.active_memtable, &mut group);
        let batch_group = group.as_slice();
        let total_ops: u64 = batch_group
            .iter()
            .map(|&req_ptr| {
                // SAFETY: request pointers remain valid until the leader marks
                // them done; callers wait on the write queue for that signal.
                unsafe { (*req_ptr).batch.len() as u64 }
            })
            .sum();
        if total_ops == 0 {
            if need_sync {
                if let Err(e) = crate::profile::timed!(
                    wal_sync_ns,
                    wal_syncs,
                    inner.wal_writer.as_mut().expect("writable WAL").sync_data()
                ) {
                    self.set_bg_error(format!("WAL fence failed: {e}"));
                    return Err(e);
                }
                self.synced_wal_seq
                    .store(self.read.current_sequence(), Ordering::Release);
            }
            return Ok(false);
        }

        let first_group_seq = self.sequence.load(Ordering::Acquire);
        let last_group_seq = first_group_seq
            .checked_add(total_ops)
            .and_then(|next| next.checked_sub(1))
            .ok_or_else(|| {
                Error::invalid_argument("sequence number space exhausted".to_string())
            })?;
        if last_group_seq > MAX_SEQUENCE_NUMBER {
            return Err(Error::invalid_argument(
                "sequence number space exhausted".to_string(),
            ));
        }
        self.sequence.store(last_group_seq + 1, Ordering::Release);

        let mut assigned =
            smallvec::SmallVec::<[(*mut WriteRequest, u64); 16]>::with_capacity(batch_group.len());
        let mut append_error: Option<Error> = None;
        let mut next_seq = first_group_seq;

        for &req_ptr in batch_group {
            // SAFETY: request pointers are owned by waiting writer stack frames
            // and these fields are immutable while queued.
            let (batch, durability) = unsafe { (&(*req_ptr).batch, (*req_ptr).durability) };
            let first_seq = next_seq;
            next_seq += batch.len() as u64;

            if durability != DurabilityMode::DisableWal {
                let header = batch.wal_header(first_seq);
                if let Some(ref mut wal) = inner.wal_writer
                    && let Err(e) = crate::profile::timed!(
                        wal_append_ns,
                        wal_appends,
                        wal.add_record_parts(&[&header, &batch.payload])
                    )
                {
                    append_error = Some(e.context("WAL write failed"));
                    break;
                }
            }
            assigned.push((req_ptr, first_seq));
            need_sync |= durability == DurabilityMode::WalSync;
        }

        // Single fsync for all batches
        let any_wal = assigned.iter().any(|&(p, _)| {
            // SAFETY: request pointers are still owned by this leader.
            unsafe { (*p).durability != DurabilityMode::DisableWal }
        });
        let wal_sync_err = if any_wal || need_sync {
            match inner.wal_writer {
                Some(ref mut wal) => {
                    let r = if need_sync {
                        crate::profile::timed!(wal_sync_ns, wal_syncs, wal.sync_data())
                    } else {
                        crate::profile::timed!(wal_flush_ns, wal_flushes, wal.flush())
                            .inspect(|_| wal.writeback_hint(self.options.wal_bytes_per_sync))
                    };
                    r.err()
                }
                None => None,
            }
        } else {
            None
        };
        if let Some(e) = wal_sync_err {
            self.set_bg_error(format!("WAL sync failed: {}", e));
            for &rp in batch_group {
                // SAFETY: the owner is blocked until `STATE_DONE`.
                unsafe { (*rp).result = Some(Err(e.clone())) };
            }
            return Err(e);
        }

        {
            crate::profile::time_scope!(apply_ns);
            let mut applied_last_seq = None;
            let mut group_bytes = 0u64;
            for &(req_ptr, first_seq) in &assigned {
                // SAFETY: request pointers are still owned by this leader.
                let batch = unsafe { &(*req_ptr).batch };
                if !batch.is_empty() {
                    group_bytes += inner.active_memtable.put_batch(batch, first_seq);
                    applied_last_seq = Some(first_seq + batch.len() as u64 - 1);
                }
            }
            if group_bytes != 0 {
                self.read.stats.record_write(group_bytes);
            }
            if let Some(last_seq) = applied_last_seq {
                self.read
                    .committed_sequence
                    .store(last_seq, Ordering::Release);
                if need_sync {
                    self.synced_wal_seq.store(last_seq, Ordering::Release);
                }
            }
        }

        if let Some(e) = append_error {
            self.set_bg_error(e.to_string());
            let mut assigned_iter = assigned.iter().map(|&(p, _)| p);
            for &rp in batch_group {
                if assigned_iter.next().is_some_and(|p| p == rp) {
                    // SAFETY: the owner is blocked until `STATE_DONE`.
                    unsafe { (*rp).result = Some(Ok(())) };
                } else {
                    // SAFETY: as above.
                    unsafe { (*rp).result = Some(Err(e.clone())) };
                }
            }
            return Ok(false);
        }

        let approx_size = inner.active_memtable.approximate_size();
        let freeze = approx_size >= self.options.write_buffer_size;
        let arm_standby = !freeze
            && approx_size >= self.options.write_buffer_size / 2
            && !self.flush_queue.standby_armed.load(Ordering::Relaxed)
            && !self.flush_queue.standby_armed.swap(true, Ordering::AcqRel);
        drop(inner);
        if arm_standby {
            self.flush_queue.request_standby();
        }
        if freeze {
            // Every batch in this group is already in WAL_old and visible in
            // active_memtable via committed_sequence. Complete and wake group
            // followers BEFORE running `old_wal.sync_data()` inside
            // `submit_active()`, while keeping `leader_active` held across the
            // rotation. Any rotation/queue failure latches fail-stop bg_error.
            Self::complete_write_group(orig_batch_group, self_ptr, &Ok(()));
            *followers_completed = true;
            return Ok(self.submit_active().is_ok());
        }
        Ok(false)
    }

    fn begin_maintenance(&self) -> Result<MaintenanceGuard<'_>> {
        let mut state = self.maintenance.lock();
        if state.closing {
            return Err(Error::db_closed());
        }
        self.check_writable()?;
        state.active += 1;
        Ok(MaintenanceGuard(self))
    }

    fn flush_generation(&self) -> Result<()> {
        let mut barrier = self.acquire_write_barrier();
        self.check_writable()?;
        barrier.claim_leadership();
        drop(barrier);
        let target = self.submit_active();
        self.release_write_leadership();
        self.flush_queue.wait(target?)
    }

    fn submit_active(&self) -> Result<u64> {
        if self.inner.lock().active_memtable.is_empty() {
            return Ok(self.flush_queue.target());
        }
        self.flush_queue
            .check_capacity(self.options.max_immutable_memtables, false)?;
        match self.freeze_memtable() {
            Ok(frozen) => Ok(self.flush_queue.submit(frozen)),
            Err(e) => {
                self.set_bg_error(format!("WAL seal or creation failed: {e}"));
                Err(e)
            }
        }
    }

    // The caller owns write leadership or is closing the database. Other threads
    // may install SSTs and take snapshots while these file operations run.
    fn freeze_memtable(&self) -> Result<FrozenMemtable> {
        crate::profile::time_scope!(freeze_ns, freezes);
        let mut standby = self.flush_queue.take_standby_wal();
        let (new_wal_number, mut old_wal) = {
            let mut inner = self.inner.lock();
            if let Some((num, _)) = &standby
                && *num <= inner.wal_number
                && let Some((stale_num, stale_writer)) = standby.take()
            {
                drop(stale_writer);
                let _ = fs::remove_file(self.storage_path.join(format!("{stale_num:06}.wal")));
            }
            let num = match &standby {
                Some((num, _)) => *num,
                None => inner.versions.file_numbers.allocate()?,
            };
            (num, inner.wal_writer.take().expect("writable WAL"))
        };
        let new_wal = match old_wal.sync_data() {
            Ok(()) => match standby.take() {
                Some((_, ready)) => Ok(ready),
                None => {
                    let path = self.storage_path.join(format!("{new_wal_number:06}.wal"));
                    WalWriter::new(&path)
                }
            },
            Err(e) => {
                if let Some((num, ready)) = standby.take() {
                    self.flush_queue.restore_standby_wal(num, ready);
                }
                Err(e)
            }
        };
        let mut inner = self.inner.lock();
        let new_wal = match new_wal {
            Ok(w) => w,
            Err(e) => {
                inner.wal_writer = Some(old_wal);
                return Err(e);
            }
        };
        let old_mem = mem::replace(&mut inner.active_memtable, Arc::new(MemTable::new()));
        old_mem.mark_immutable();
        let old_wal_number = mem::replace(&mut inner.wal_number, new_wal_number);
        inner.wal_writer = Some(new_wal);
        inner.immutable_memtables.push_front(old_mem.clone());
        inner.l0_install_epoch += 1;
        let last_sequence = self.read.current_sequence();
        self.synced_wal_seq.store(last_sequence, Ordering::Release);
        self.install_super_version(&inner);
        Ok(FrozenMemtable {
            old_mem,
            old_wal_number,
            new_wal_number,
            last_sequence,
            file_numbers: inner.versions.file_numbers.clone(),
            snapshots: self.snapshot_list.as_sorted_vec(),
        })
    }

    fn flush_context(&self) -> FlushContext {
        FlushContext {
            inner: self.inner.clone(),
            storage_path: self.storage_path.clone(),
            dir_sync: self.dir_sync.clone(),
            options: self.options.clone(),
            builds: self.builds.clone(),
            stats: self.read.stats.clone(),
            table_cache: self.table_cache.clone(),
            claims: self.compacting_files.clone(),
            super_version: self.read.super_version.clone(),
            l0_file_count: self.l0_file_count.clone(),
            compaction_notify: self.read.compaction_notify.clone(),
            manifest_poisoned: self.read.manifest_poisoned.clone(),
            has_bg_error: self.read.has_bg_error.clone(),
            bg_error: self.read.bg_error.clone(),
        }
    }

    /// Drain L0 down below its compaction trigger. Follows the same
    /// short-lock pick → unlocked I/O → short-lock install → unlocked sync
    /// pattern used by the background compaction thread pool
    /// (`docs/ai/patterns/concurrency.md` INV-CC4), so this does not
    /// stall other `db_mutex` consumers (writers, other compactions, admin
    /// calls) for the duration of the merge I/O — unlike the previous
    /// `do_compaction`, which held the lock for its entire run. Used by the
    /// inline write-throttle path and by `flush()`'s post-flush trigger,
    /// neither of which should pay for a full-database compaction, and by
    /// `force_compact_all`.
    ///
    /// When a pick is blocked because another in-flight compaction has
    /// claimed the current L0 files, `wait_for_inflight` selects the policy:
    /// `false` returns immediately (opportunistic callers such as flush's
    /// post-trigger drain — the in-flight compaction is already doing the
    /// work), while `true` waits for the claim to settle and re-picks so L0
    /// is genuinely drained on return. The L0 stop trigger and
    /// `force_compact_all` require `true`: returning while the claim is held
    /// leaves L0 at or above the stop count, and later flushes then add files
    /// without bound.
    fn drain_l0(&self, wait_for_inflight: bool) -> Result<()> {
        let force_opts = DbOptions {
            l0_compaction_trigger: 1,
            ..self.options.clone()
        };
        let mut installed = false;
        loop {
            // Phase 1: pick and claim (short lock). Only L0 work: deeper
            // levels are left to the background workers.
            let (pick, l0_blocked) = {
                let inner = self.inner.lock();
                let version = inner.versions.current();
                let mut claimed = self.compacting_files.lock();
                match LeveledCompaction::pick_l0_compaction(&version, &claimed) {
                    Some(task) => {
                        let l0_inputs: Vec<u64> = task
                            .input_files_level
                            .iter()
                            .map(|f| f.meta.number)
                            .collect();
                        let file_numbers = inner.versions.file_numbers.clone();
                        let all_inputs: Vec<_> = task
                            .input_files_level
                            .iter()
                            .chain(task.input_files_next.iter())
                            .cloned()
                            .collect();
                        let is_bottom = LeveledCompaction::is_bottommost_level(
                            &version,
                            task.level,
                            force_opts.num_levels,
                            &all_inputs,
                        );
                        let claimed_numbers: Vec<u64> =
                            all_inputs.iter().map(|f| f.meta.number).collect();
                        claimed.extend(claimed_numbers.iter().copied());
                        // Capture the snapshot list under the DB lock,
                        // consistent with the inputs just picked.
                        let active_snaps = self.snapshot_list.as_sorted_vec();
                        (
                            Some((
                                task,
                                file_numbers,
                                l0_inputs,
                                is_bottom,
                                active_snaps,
                                claimed_numbers,
                            )),
                            false,
                        )
                    }
                    // A `None` pick while L0 is non-empty means an in-flight
                    // claim holds L0 files — not that L0 is drained.
                    None => (None, version.l0_file_count() > 0),
                }
            }; // lock released
            let Some((task, file_numbers, l0_inputs, is_bottom, active_snaps, claimed_numbers)) =
                pick
            else {
                if wait_for_inflight && l0_blocked {
                    // Wait (without holding `inner`) for the in-flight
                    // compaction to install or discard, then re-pick.
                    // Breaking here instead would return with L0 undrained.
                    self.compacting_files.wait_until_empty();
                    continue;
                }
                break;
            };
            // Claim released on every exit (success or `?`) via Drop —
            // dropped before the next loop iteration re-picks.
            let _claim = CompactionClaim {
                claims: Arc::clone(&self.compacting_files),
                numbers: claimed_numbers,
            };

            // Phase 2: I/O (no lock held)
            let ctx = CompactionContext {
                db_path: &self.storage_path,
                dir_sync: &self.dir_sync,
                manifest_poisoned: &self.read.manifest_poisoned,
                options: &force_opts,
                builds: &self.builds,
                rate_limiter: Some(&self.rate_limiter),
                stats: Some(&self.read.stats),
                active_snapshots: &active_snaps,
            };
            let output =
                LeveledCompaction::execute_compaction_io(&ctx, &task, &file_numbers, is_bottom)
                    .ctx()?;

            // Pre-warm the table cache for the new output files while
            // unlocked (see the background compaction loop for rationale).
            self.table_cache
                .prewarm(output.edit.new_files.iter().map(|(_, meta)| meta.number));

            // Phase 3: install (short lock)
            let cleanup = {
                let mut inner = self.inner.lock();
                let cleanup = LeveledCompaction::install_compaction(
                    output,
                    &mut inner.versions,
                    Some(&self.table_cache),
                    Some(&self.block_cache),
                    &self.storage_path,
                    Some(&self.read.stats),
                    !self.snapshot_list.is_empty(),
                )
                .ctx()?;
                // Unpin L0 inputs that actually left L0. Covers both a
                // normal install (input consumed and scheduled for deletion)
                // and a trivial move (same physical file relabeled to L1, so
                // not in files_to_delete). A discarded (stale, raced) install
                // leaves the file in L0 and its first-block pin must stay.
                if !l0_inputs.is_empty() {
                    let current = inner.versions.current();
                    for num in &l0_inputs {
                        if !current
                            .level_files(0)
                            .iter()
                            .any(|tf| tf.meta.number == *num)
                        {
                            self.block_cache.unpin_file(*num);
                        }
                    }
                }
                self.l0_file_count
                    .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
                self.install_super_version(&inner);
                cleanup
            };
            // Phase 4: sync manifest + delete old SSTs (no lock)
            if let Err(e) = {
                let manifest_handle = self.inner.lock().versions.manifest_sync_handle();
                confirm_manifest_durable(&manifest_handle, &self.read.manifest_poisoned)
            } {
                // The edit is applied in memory but its durability could
                // not be confirmed — the same condition the background
                // thread treats as fail-stop. confirm_manifest_durable
                // enforces poison; record it so `flush()` and `check_usable`
                // see a fatal engine state, not a retryable hiccup.
                self.set_bg_error(format!("drain_l0 manifest sync failed: {}", e));
                return Err(e).ctx();
            }
            LeveledCompaction::run_post_compaction_cleanup(&cleanup, &self.storage_path);
            installed = true;
        }
        // The drained L0 data may have pushed L1+ over budget.
        if installed {
            self.read.signal_compaction();
        }
        Ok(())
    }

    fn level_rewrite(&self) -> LevelRewrite<'_> {
        LevelRewrite {
            inner: &self.inner,
            snapshots: &self.snapshot_list,
            claims: &self.compacting_files,
            table_cache: &self.table_cache,
            block_cache: &self.block_cache,
            super_version: &self.read.super_version,
            l0_count: &self.l0_file_count,
        }
    }

    /// Drain L0 then rewrite each populated level outside the DB mutex.
    fn force_compact_all(&self) -> Result<()> {
        // Settle in-flight background compactions first. Their merge streams
        // were filtered when they were picked, so decisions may predate
        // dead-key registrations (or other filter-state changes) made before
        // this call. Waiting lets them install — their outputs are then
        // rewritten by the passes below — or discard, instead of installing
        // stale-filtered data after this full pass has returned.
        self.compacting_files.wait_until_empty();
        self.drain_l0(true)?;
        let ctx = CompactionContext {
            db_path: &self.storage_path,
            dir_sync: &self.dir_sync,
            manifest_poisoned: &self.read.manifest_poisoned,
            options: &self.options,
            builds: &self.builds,
            rate_limiter: Some(&self.rate_limiter),
            stats: Some(&self.read.stats),
            active_snapshots: &[],
        };
        for level in (1..self.options.num_levels).rev() {
            self.level_rewrite().run(level, &ctx, false)?;
        }
        Ok(())
    }

    /// Remove files in the DB directory that recovery has proven dead:
    /// - `.wal` files numbered below the recovered `log_number` — already
    ///   replayed (or superseded) and never read again,
    /// - `.sst` files absent from the recovered version — crash orphans from
    ///   an interrupted flush/compaction, or compaction inputs whose deletion
    ///   edit was durable but whose unlink never ran,
    /// - `MANIFEST-*` files other than the current one, plus
    ///   numbered `CURRENT.tmp.<N>` files from interrupted MANIFEST publication.
    ///
    /// Called from `open()` after the recovered state (including the fresh
    /// `log_number`) is durable in the MANIFEST, while the directory LOCK is
    /// held and before any background thread starts. Deletion failures are
    /// logged and ignored — cleanup re-runs on the next open.
    fn remove_orphan_files(path: &Path, versions: &VersionSet) {
        let version = versions.current();
        let live_ssts: HashSet<u64> = (0..version.num_levels)
            .flat_map(|level| version.level_files(level))
            .map(|tf| tf.meta.number)
            .collect();

        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("orphan cleanup: cannot read DB dir: {}", e);
                return;
            }
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let obsolete = if let Some(num) = name
                .strip_suffix(".wal")
                .and_then(|s| s.parse::<u64>().ok())
            {
                num < versions.log_number()
            } else if let Some(num) = name
                .strip_suffix(".sst")
                .and_then(|s| s.parse::<u64>().ok())
            {
                !live_ssts.contains(&num)
            } else if let Some(num) = name
                .strip_prefix("MANIFEST-")
                .and_then(|s| s.parse::<u64>().ok())
            {
                num != versions.manifest_number()
            } else {
                name.strip_prefix("CURRENT.tmp.")
                    .is_some_and(|n| n.parse::<u64>().is_ok())
            };
            if obsolete {
                let file_path = entry.path();
                match fs::remove_file(&file_path) {
                    Ok(()) => tracing::info!("removed orphan file {}", file_path.display()),
                    Err(e) => {
                        tracing::warn!(
                            "failed to remove orphan file {}: {}",
                            file_path.display(),
                            e
                        );
                    }
                }
            }
        }
    }

    fn replay_wal_record(
        data: &[u8],
        mem: &MemTable,
        last_wal_seq: &mut u64,
        max_sequence: &mut u64,
    ) -> Result<()> {
        if data.len() < 12 {
            return Err(Error::corruption(format!(
                "WAL record too short: {} bytes",
                data.len()
            )));
        }
        let seq = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let count = u32::from_le_bytes(data[8..12].try_into().unwrap());
        if count == 0 {
            return Err(Error::corruption("WAL batch record has zero entries"));
        }
        if seq == 0 || seq <= *last_wal_seq {
            return Err(Error::corruption(format!(
                "WAL record sequence {seq} is not strictly greater than previous sequence {}",
                *last_wal_seq
            )));
        }
        let mut offset = 12;

        for i in 0..count {
            if offset >= data.len() {
                return Err(Error::corruption(format!(
                    "WAL record truncated at entry {}/{}",
                    i, count
                )));
            }
            let entry_seq = seq.checked_add(i as u64).ok_or_else(|| {
                Error::corruption("WAL record sequence number overflow".to_string())
            })?;
            if entry_seq > MAX_SEQUENCE_NUMBER {
                return Err(Error::corruption(format!(
                    "WAL record sequence {} exceeds max {}",
                    entry_seq, MAX_SEQUENCE_NUMBER
                )));
            }

            let vt = data[offset];
            offset += 1;
            let key_len_end = offset
                .checked_add(4)
                .filter(|&end| end <= data.len())
                .ok_or_else(|| {
                    Error::corruption(format!(
                        "WAL record truncated reading key length at entry {i}"
                    ))
                })?;
            let key_len =
                u32::from_le_bytes(data[offset..key_len_end].try_into().unwrap()) as usize;
            offset = key_len_end;
            let key_end = offset
                .checked_add(key_len)
                .filter(|&end| end <= data.len())
                .ok_or_else(|| {
                    Error::corruption(format!("WAL record truncated reading key at entry {i}"))
                })?;
            if key_len > MAX_USER_KEY_SIZE {
                return Err(Error::corruption(format!(
                    "WAL record key length {key_len} exceeds maximum {MAX_USER_KEY_SIZE}"
                )));
            }
            let key = &data[offset..key_end];
            offset = key_end;

            match ValueType::from_u8(vt) {
                Some(ValueType::Value) => {
                    let val_len_end = offset
                        .checked_add(4)
                        .filter(|&end| end <= data.len())
                        .ok_or_else(|| {
                            Error::corruption(format!(
                                "WAL record truncated reading value length at entry {i}"
                            ))
                        })?;
                    let val_len =
                        u32::from_le_bytes(data[offset..val_len_end].try_into().unwrap()) as usize;
                    offset = val_len_end;
                    let val_end = offset
                        .checked_add(val_len)
                        .filter(|&end| end <= data.len())
                        .ok_or_else(|| {
                            Error::corruption(format!(
                                "WAL record truncated reading value at entry {i}"
                            ))
                        })?;
                    if key_len.saturating_add(val_len) > MAX_WRITE_ENTRY_SIZE {
                        return Err(Error::corruption(format!(
                            "WAL record entry size exceeds maximum {MAX_WRITE_ENTRY_SIZE}"
                        )));
                    }
                    let value = &data[offset..val_end];
                    offset = val_end;
                    mem.put(key, value, entry_seq, ValueType::Value);
                }
                Some(ValueType::Deletion) => {
                    mem.put(key, &[], entry_seq, ValueType::Deletion);
                }
                Some(ValueType::RangeDeletion) => {
                    // RangeDeletion: value is the end key
                    let val_len_end = offset
                        .checked_add(4)
                        .filter(|&end| end <= data.len())
                        .ok_or_else(|| {
                            Error::corruption(format!(
                                "WAL record truncated reading range-del end key length at entry {i}"
                            ))
                        })?;
                    let val_len =
                        u32::from_le_bytes(data[offset..val_len_end].try_into().unwrap()) as usize;
                    offset = val_len_end;
                    let val_end = offset
                        .checked_add(val_len)
                        .filter(|&end| end <= data.len())
                        .ok_or_else(|| {
                            Error::corruption(format!(
                                "WAL record truncated reading range-del end key at entry {i}"
                            ))
                        })?;
                    if val_len > MAX_USER_KEY_SIZE
                        || key_len.saturating_add(val_len) > MAX_RANGE_DELETE_SIZE
                    {
                        return Err(Error::corruption(format!(
                            "WAL range deletion size exceeds maximum {MAX_RANGE_DELETE_SIZE}"
                        )));
                    }
                    let value = &data[offset..val_end];
                    if key >= value {
                        return Err(Error::corruption(
                            "WAL range deletion has empty or inverted bounds",
                        ));
                    }
                    offset = val_end;
                    mem.put(key, value, entry_seq, ValueType::RangeDeletion);
                }
                None => {
                    return Err(Error::corruption(format!(
                        "WAL record contains unknown value type {} at entry {}",
                        vt, i
                    )));
                }
            }
            *last_wal_seq = entry_seq;
            *max_sequence = (*max_sequence).max(entry_seq);
        }
        if offset != data.len() {
            return Err(Error::corruption(format!(
                "WAL batch contains {} trailing bytes after {count} entries",
                data.len() - offset
            )));
        }
        Ok(())
    }

    /// Build options for flush outputs (always L0).
    /// Write a memtable to one or more SST files, splitting at user-key
    /// boundaries whenever projected single-block metadata reaches
    /// `META_BLOCK_SPLIT_THRESHOLD` (the SST reader rejects
    /// any block above 64 MiB, so a single huge output for key-heavy data
    /// would be unreadable on read-back). Outputs cover disjoint user-key
    /// ranges, so all versions of one user key stay in a single file — this
    /// is required for correct newest-file-first L0 point lookups.
    ///
    /// On error, all already-written output files are removed.
    fn write_memtable_ssts(
        mem: &MemTable,
        db_path: &Path,
        make_opts: &dyn Fn() -> TableBuildOptions,
        file_numbers: &FileNumbers,
        snapshots: &[u64],
    ) -> Result<Vec<(u64, TableBuildResult)>> {
        let pending = PendingSsts::new(db_path, file_numbers);
        let mut results: Vec<(u64, TableBuildResult)> = Vec::new();
        let mut builder: Option<(u64, TableBuilder)> = None;
        let mut carry = Vec::new();
        let mut pending_cut = false;
        let mut last_uk: Vec<u8> = Vec::new();
        let mut has_last_uk = false;
        let mut last_point_uk: Vec<u8> = Vec::new();
        let mut has_last_point_uk = false;
        let mut last_written_point_seq = 0u64;
        let mut snapshot_idx = 0usize;

        for (key, value) in mem.iter_ref() {
            let (uk, entry_seq, vt) = types::decode_internal_key(key).ctx()?;
            if vt != ValueType::RangeDeletion {
                if has_last_point_uk && last_point_uk.as_slice() == uk {
                    while snapshot_idx > 0 && snapshots[snapshot_idx - 1] >= last_written_point_seq
                    {
                        snapshot_idx -= 1;
                    }
                    if snapshot_idx > 0 && snapshots[snapshot_idx - 1] >= entry_seq {
                        last_written_point_seq = entry_seq;
                    } else {
                        // Completely shadowed by `last_written_point_seq` with no
                        // intervening live snapshot.
                        continue;
                    }
                } else {
                    last_point_uk.clear();
                    last_point_uk.extend_from_slice(uk);
                    has_last_point_uk = true;
                    last_written_point_seq = entry_seq;
                    snapshot_idx = snapshots.len();
                }
            }

            let uk_changed = !has_last_uk || uk != last_uk.as_slice();
            if uk_changed {
                last_uk.clear();
                last_uk.extend_from_slice(uk);
                has_last_uk = true;
            }
            if pending_cut
                && uk_changed
                && let Some((num, mut b)) = builder.take()
            {
                carry = b.clip_at(uk);
                results.push((num, b.finish().ctx()?));
                pending_cut = false;
            }
            if builder.is_none() {
                let num = pending.allocate().ctx()?;
                let sst_path = db_path.join(format!("{:06}.sst", num));
                let mut output = TableBuilder::new(&sst_path, make_opts()).ctx()?;
                output.start_output(snapshots, mem::take(&mut carry));
                builder = Some((num, output));
            }
            let (_, b) = builder.as_mut().unwrap();
            b.add(key, value).ctx()?;
            if b.projected_meta_size() >= META_BLOCK_SPLIT_THRESHOLD {
                pending_cut = true;
            }
        }
        if let Some((num, b)) = builder.take() {
            results.push((num, b.finish().ctx()?));
        }
        pending.keep();
        Ok(results)
    }
}

/// Test utilities gated behind the `test-utils` feature.
#[cfg(any(test, feature = "test-utils"))]
impl DB {
    /// Simulate a crash: shut down background threads without flushing memtable.
    /// Useful for testing WAL recovery without lingering background compaction threads.
    pub fn simulate_crash(self) {
        self.read.closed.store(true, Ordering::Release);
        self.stop_background_and_detach_cache();
        self.flush_queue.abandon_standby_wal_for_crash();
        let mut inner = self.inner.lock();
        if let Some(wal) = inner.wal_writer.take() {
            wal.discard_buffer();
        }
        if let Some(manifest) = inner.versions.manifest_sync_handle().lock().take() {
            manifest.discard_buffer();
        }
        self.dir_sync.lock().take();
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        if !self.read.closed.load(Ordering::Acquire) {
            self.read.closed.store(true, Ordering::Release);
            if let Some(ref mut wal) = self.inner.lock().wal_writer {
                // Note: WAL sync errors in Drop cannot be propagated to the caller.
                // Users should call db.close() explicitly to detect and handle sync errors.
                let _ = wal.sync();
            }
        }
        let current_id = thread::current().id();
        let on_worker_or_callback = self.builds.contains_current_thread()
            || self
                .flush_worker
                .lock()
                .as_ref()
                .is_some_and(|h| h.thread().id() == current_id)
            || self
                .compaction_handles
                .lock()
                .iter()
                .any(|h| h.thread().id() == current_id);
        if on_worker_or_callback {
            self.flush_queue.stop();
            {
                let (lock, cvar) = &*self.read.compaction_notify;
                let _guard = lock.lock();
                self.compaction_shutdown.store(true, Ordering::Release);
                cvar.notify_all();
            }
            drop(self.flush_worker.lock().take());
            drop(mem::take(&mut *self.compaction_handles.lock()));
            self.worker_lifecycle.defer_teardown(DeferredTeardown {
                flush_queue: Arc::clone(&self.flush_queue),
                storage_path: self.storage_path.clone(),
                block_cache: Arc::clone(&self.block_cache),
                table_cache: Arc::clone(&self.table_cache),
                inner: Arc::clone(&self.inner),
                super_version: Arc::clone(&self.read.super_version),
                dir_sync: Arc::clone(&self.dir_sync),
                lock_file: self.lock_file.lock().take(),
            });
            return;
        }
        self.stop_background_and_detach_cache();
        self.worker_lifecycle.wait_until_stopped();
        DeferredTeardown {
            flush_queue: Arc::clone(&self.flush_queue),
            storage_path: self.storage_path.clone(),
            block_cache: Arc::clone(&self.block_cache),
            table_cache: Arc::clone(&self.table_cache),
            inner: Arc::clone(&self.inner),
            super_version: Arc::clone(&self.read.super_version),
            dir_sync: Arc::clone(&self.dir_sync),
            lock_file: self.lock_file.lock().take(),
        }
        .run();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn lazy_delete_batch_sizes_the_registry_by_distinct_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.lazy_delete_batch(std::iter::repeat_n(b"k", 4096));
        db.lazy_delete_batch(std::iter::repeat_n(b"k", 32 * 1024));
        let keys = db.dead_keys.read();
        assert_eq!(keys.len(), 1);
        assert!(
            keys.capacity() < 128,
            "duplicate input reserved {} slots",
            keys.capacity()
        );
    }

    #[test]
    fn lazy_delete_singleton_registers_after_the_input_iterator_drops() {
        struct ClearOnDrop<'a>(&'a DB);
        impl Drop for ClearOnDrop<'_> {
            fn drop(&mut self) {
                self.0.clear_dead_keys();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.lazy_delete(b"old");
        let clear = ClearOnDrop(&db);
        let mut one = Some(b"new");
        db.lazy_delete_batch(std::iter::from_fn(move || {
            let _keep_until_drop = &clear;
            one.take()
        }));
        assert!(db.dead_keys.read().contains(b"new".as_slice()));
        assert_eq!(db.dead_key_count(), 1);
    }

    #[test]
    fn lazy_delete_batch_publishes_mixed_size_keys_after_reentrant_clear() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.lazy_delete(b"prior");
        let large = vec![7; 64 * 1024];
        let keys = [b"".as_slice(), b"ab", &large, b"ab", b"tail"];
        db.lazy_delete_batch(keys.into_iter().enumerate().map(|(i, key)| {
            if i == 3 {
                assert_eq!(db.dead_key_count(), 1);
                db.clear_dead_keys();
                db.lazy_delete(b"callback");
            }
            key
        }));
        let registered = db.dead_keys.read();
        assert_eq!(registered.len(), 5);
        for key in keys.into_iter().chain([b"callback".as_slice()]) {
            assert!(registered.contains(key));
        }
    }

    #[test]
    fn batch_iterator_hides_future_commits_at_the_overlay_sequence() {
        for with_range in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = open_test_db(dir.path());
            let mut overlay = WriteBatchWithIndex::new();
            if with_range {
                overlay.delete_range(b"a", b"b");
            }
            overlay.put(b"x", b"overlay");
            db.sequence.store(
                MAX_SEQUENCE_NUMBER - overlay.operation_count(),
                Ordering::Release,
            );
            db.put(b"base", b"visible").unwrap();
            let forward = db.iter_with_batch(&overlay).unwrap();
            let mut reverse = db.iter_with_batch(&overlay).unwrap();
            // The first future commit has exactly the lowest overlay sequence.
            db.put(b"z", b"future").unwrap();
            let keys: Vec<_> = forward.map(|(key, _)| key).collect();
            assert_eq!(keys, [b"base".to_vec(), b"x".to_vec()]);
            reverse.seek_to_last();
            assert_eq!(reverse.key(), Some(b"x".as_slice()));
            reverse.prev();
            assert_eq!(reverse.key(), Some(b"base".as_slice()));
            if with_range {
                db.put(b"zz", b"last").unwrap();
            }
            assert_eq!(db.read.current_sequence(), MAX_SEQUENCE_NUMBER);
            assert_eq!(
                db.iter_with_batch(&WriteBatchWithIndex::new())
                    .unwrap()
                    .count(),
                db.iter().unwrap().count()
            );
        }
    }

    #[test]
    fn options_upper_bound_prunes_unrelated_l0_prefetch() {
        use crate::sst::table_reader::WILLNEED_HINTS;

        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            pin_l0_first_block_and_warm_index: false,
            l0_compaction_trigger: usize::MAX,
            l0_slowdown_trigger: usize::MAX,
            l0_stop_trigger: usize::MAX,
            ..Default::default()
        };
        let db = DB::open(options, dir.path()).unwrap();
        for key in [b"a", b"p", b"z"] {
            db.put(key, b"v").unwrap();
            db.flush().unwrap();
        }
        let options = ReadOptions {
            iterate_upper_bound: Some(b"p".as_slice().into()),
            ..Default::default()
        };
        WILLNEED_HINTS.with(|count| count.set(0));
        let keys: Vec<_> = db
            .iter_with_options(&options)
            .unwrap()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, vec![b"a".to_vec()]);
        assert_eq!(WILLNEED_HINTS.with(|count| count.get()), 1);
    }

    #[test]
    fn effective_bounds_preserve_cross_file_tombstones_and_empty_seeks() {
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            pin_l0_first_block_and_warm_index: false,
            l0_compaction_trigger: usize::MAX,
            l0_slowdown_trigger: usize::MAX,
            l0_stop_trigger: usize::MAX,
            ..Default::default()
        };
        let db = DB::open(options, dir.path()).unwrap();
        for key in [b"p:1", b"p:2", b"p:3", b"p:4"] {
            db.put(key, b"v").unwrap();
        }
        db.flush().unwrap();
        let snapshot = db.snapshot();
        // This file's only key is 'a'; pruning its point source must still
        // retain the deletion reaching into the p: range in the older file.
        db.delete_range(b"a", b"p:3").unwrap();
        db.flush().unwrap();
        let options = ReadOptions {
            iterate_lower_bound: Some(b"p:2".as_slice().into()),
            iterate_upper_bound: Some(b"p:4".as_slice().into()),
            ..Default::default()
        };
        let collect = |iter: DBIterator| iter.map(|(key, _)| key).collect::<Vec<_>>();
        assert_eq!(
            collect(db.iter_with_options(&options).unwrap()),
            vec![b"p:3".to_vec()]
        );
        assert_eq!(
            collect(db.iter_with_prefix(b"p:", &options).unwrap()),
            vec![b"p:3".to_vec()]
        );
        assert_eq!(
            collect(
                db.iter_with_range(&options, Some(b"p:1"), Some(b"p:3"))
                    .unwrap()
            ),
            Vec::<Vec<u8>>::new()
        );
        let old_options = ReadOptions {
            snapshot: Some((&snapshot).into()),
            ..options.clone()
        };
        assert_eq!(
            collect(db.iter_with_prefix(b"p:", &old_options).unwrap()),
            vec![b"p:2".to_vec(), b"p:3".to_vec()]
        );
        assert_eq!(db.get(b"p:2").unwrap(), None);

        let misses = db.read.stats.block_cache_misses.load(Ordering::Relaxed);
        for (prefix, lower, upper) in [
            (b"p:".as_slice(), b"z".as_slice(), b"zz".as_slice()),
            (b"p:", b"", b"a"),
            (b"p:", b"p:2", b"p:2"),
            (b"", b"z", b"a"),
            (b"\xff", b"", b"\xff"),
        ] {
            let options = ReadOptions {
                iterate_lower_bound: Some(lower.into()),
                iterate_upper_bound: Some(upper.into()),
                ..Default::default()
            };
            let mut iter = db.iter_with_prefix(prefix, &options).unwrap();
            assert!(iter.next().is_none());
            iter.seek_to_first();
            assert!(!iter.valid());
            iter.seek_to_last();
            assert!(!iter.valid());
            iter.seek(b"p:2");
            assert!(!iter.valid());
            iter.seek_for_prev(b"p:4");
            assert!(!iter.valid());
            assert!(iter.error().is_none());
        }
        assert_eq!(
            db.read.stats.block_cache_misses.load(Ordering::Relaxed),
            misses
        );
    }

    #[test]
    fn range_write_admission_reserves_latest_stripe_and_rejects_only_its_request() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.snapshot_list
            .range_stripe_limit
            .store(3, Ordering::Relaxed);

        db.delete_range(b"a", b"z").unwrap();
        let duplicates: Vec<_> = (0..9).map(|_| db.snapshot()).collect();
        db.delete_range(b"a", b"z").unwrap();
        let _second = db.arc_snapshot();
        db.delete_range(b"a", b"z").unwrap();
        let _third = db.snapshot();
        let seq = db.read.current_sequence();
        let wal = dir
            .path()
            .join(format!("{:06}.wal", db.inner.lock().wal_number));
        let before = fs::read(&wal).unwrap();
        assert_eq!(
            db.delete_range(b"a", b"z").unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(db.read.current_sequence(), seq);
        assert_eq!(fs::read(&wal).unwrap(), before);

        let mut mixed = WriteBatch::new();
        mixed.put(b"rejected", b"value");
        mixed.delete_range(b"a", b"z");
        let mut point = WriteBatch::new();
        point.put(b"admitted", b"value");
        let mut requests = [mixed, point].map(|batch| WriteRequest {
            batch,
            kind: WriteRequestKind::Batch,
            no_slowdown: false,
            durability: DurabilityMode::WalAsync,
            waiter: None,
            result: None,
            state: AtomicU8::new(REQ_QUEUED),
        });
        let mut completed = false;
        db.write_batch_group(
            &requests.each_mut().map(std::ptr::from_mut),
            std::ptr::null_mut(),
            &mut completed,
        )
        .unwrap();
        assert_eq!(
            requests[0].result.take().unwrap().unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(db.read.current_sequence(), seq + 1);
        assert!(db.get(b"rejected").unwrap().is_none());
        assert_eq!(
            db.get(b"admitted").unwrap().as_deref(),
            Some(b"value".as_slice())
        );

        // A fourth snapshot created after the final range delete can only
        // observe the same latest covering sequence as the third snapshot.
        let late = db.snapshot();
        db.flush().unwrap();
        let version = db.inner.lock().versions.current();
        let ranges = version.level_files(0)[0]
            .reader
            .get_range_tombstones()
            .unwrap();
        let mut seqs: Vec<_> = ranges.into_iter().map(|(_, _, s)| s).collect();
        seqs.sort_unstable();
        assert_eq!(seqs, vec![1, 2, 3]);
        drop(late);
        drop(duplicates);
        db.delete_range(b"a", b"z").unwrap();
    }

    #[test]
    fn oversized_range_delete_rejects_batch_before_wal_and_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"existing", b"value").unwrap();
        let sequence = db.sequence.load(Ordering::Acquire);
        let wal_path = dir
            .path()
            .join(format!("{:06}.wal", db.inner.lock().wal_number));
        let wal_before = fs::read(&wal_path).unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"new", b"value");
        batch.delete_range(b"a", &vec![b'z'; types::MAX_RANGE_DELETE_SIZE]);
        assert_eq!(
            db.write(batch).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(db.sequence.load(Ordering::Acquire), sequence);
        assert_eq!(fs::read(wal_path).unwrap(), wal_before);
        assert_eq!(db.get(b"new").unwrap(), None);
        assert_eq!(
            db.get(b"existing").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        db.put(b"after", b"accepted").unwrap();
    }

    #[test]
    fn group_byte_limit_preserves_fifo_and_admits_an_oversized_head() {
        let mut requests = [3, 3, 1, 20].map(|length| {
            let mut batch = WriteBatch::new();
            batch.delete(&vec![0; length]);
            WriteRequest {
                batch,
                kind: WriteRequestKind::Batch,
                no_slowdown: false,
                durability: DurabilityMode::WalAsync,
                waiter: None,
                result: None,
                state: AtomicU8::new(REQ_QUEUED),
            }
        });
        let ptrs = requests.each_mut().map(std::ptr::from_mut);
        let mut wq = WriteQueueState {
            waiting_barriers: 0,
            queue: ptrs.into(),
            leader_active: true,
        };
        // Encoded sizes are 8, 8, 6, and 25 bytes. The group limit is 16.
        assert_eq!(wq.drain_group(32).as_slice(), &ptrs[..2]);
        assert_eq!(wq.drain_group(32).as_slice(), &ptrs[2..3]);
        assert_eq!(wq.drain_group(32).as_slice(), &ptrs[3..]);
        assert!(wq.queue.is_empty());
    }

    #[test]
    fn oversized_same_key_run_rejected_before_wal_then_flush_recovers() {
        let temp = tempfile::tempdir().unwrap();
        let options = DbOptions {
            write_buffer_size: 128 << 20,
            ..Default::default()
        };
        let db = DB::open(options.clone(), temp.path()).unwrap();
        let key = vec![1; types::MAX_USER_KEY_SIZE];
        let mut batch = WriteBatch::new();
        for _ in 0..3 {
            batch.put(&key, b"value");
        }
        db.write(batch).unwrap();
        let seq = db.read.current_sequence();
        let path = temp
            .path()
            .join(format!("{:06}.wal", db.inner.lock().wal_number));
        let wal_bytes = fs::metadata(&path).unwrap().len();
        let mut batch = WriteBatch::new();
        batch.put(b"peer", b"new");
        batch.delete(&key);
        assert_eq!(
            db.write(batch).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(db.read.current_sequence(), seq);
        assert_eq!(fs::metadata(path).unwrap().len(), wal_bytes);
        assert!(db.get(b"peer").unwrap().is_none());
        db.flush().unwrap();
        db.delete(&key).unwrap();
        db.close().unwrap();
        let reopened = DB::open(options, temp.path()).unwrap();
        assert!(reopened.get(&key).unwrap().is_none());
    }

    #[test]
    fn recovery_keeps_long_same_key_runs_in_separate_wal_generations() {
        let temp = tempfile::tempdir().unwrap();
        let options = DbOptions {
            write_buffer_size: 256 << 20,
            l0_compaction_trigger: usize::MAX,
            ..Default::default()
        };
        let db = DB::open(options.clone(), temp.path()).unwrap();
        let first_wal = db.inner.lock().wal_number;
        db.simulate_crash();
        let key = vec![b'k'; types::MAX_USER_KEY_SIZE];
        // Each generation is within the admission budget. Their union
        // exceeds it, while remaining below the recovery buffer threshold.
        for generation in 0..2 {
            let mut batch = WriteBatch::new();
            for _ in 0..3 {
                batch.put(&key, &[generation as u8]);
            }
            let path = temp
                .path()
                .join(format!("{:06}.wal", first_wal + generation));
            let mut wal = WalWriter::new(&path).unwrap();
            wal.add_record_parts(&[&batch.wal_header(generation * 3 + 1), &batch.payload])
                .unwrap();
            wal.sync_data().unwrap();
        }
        let db = DB::open(options, temp.path()).unwrap();
        assert_eq!(db.read.current_sequence(), 6);
        assert_eq!(db.get(&key).unwrap().as_deref(), Some(&[1][..]));
        assert_eq!(db.inner.lock().versions.current().l0_file_count(), 2);
    }

    #[test]
    fn boundary_range_delete_flushes_with_existing_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            write_buffer_size: 512 * 1024 * 1024,
            block_cache_capacity: 0,
            pin_l0_first_block_and_warm_index: false,
            l0_compaction_trigger: usize::MAX,
            ..Default::default()
        };
        let db = DB::open(options.clone(), dir.path()).unwrap();
        for key in [b"a1", b"b1", b"c1", b"z1"] {
            db.put(key, b"value").unwrap();
        }
        let snapshot = db.snapshot();
        // Each endpoint can be as large as MAX_USER_KEY_SIZE. Multiple
        // such tombstones force rollover of the range metadata leaves.
        for begin in *b"abc" {
            let mut end = vec![begin; types::MAX_USER_KEY_SIZE];
            end[1] = u8::MAX;
            db.delete_range(&[begin], &end).unwrap();
        }
        db.flush().unwrap();
        for key in [b"a1", b"b1", b"c1"] {
            assert_eq!(db.get(key).unwrap(), None);
            assert_eq!(
                db.get_with_options(&snapshot.read_options(), key)
                    .unwrap()
                    .as_deref(),
                Some(b"value".as_slice())
            );
        }
        db.compact().unwrap();
        assert_eq!(db.get(b"z1").unwrap().as_deref(), Some(b"value".as_slice()));
        drop(snapshot);
        db.simulate_crash();
        let recovered = DB::open(options, dir.path()).unwrap();
        assert_eq!(recovered.get(b"c1").unwrap(), None);
        assert_eq!(
            recovered.get(b"z1").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
    }

    #[test]
    fn read_view_precedes_sequence_during_compaction() {
        for constructor in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let db = Arc::new(
                DB::open(
                    DbOptions {
                        l0_compaction_trigger: usize::MAX,
                        l0_slowdown_trigger: usize::MAX,
                        l0_stop_trigger: usize::MAX,
                        ..Default::default()
                    },
                    dir.path(),
                )
                .unwrap(),
            );
            // Keep the race deterministic: this test drives compaction itself.
            {
                let (lock, cv) = &*db.read.compaction_notify;
                let _guard = lock.lock();
                db.compaction_shutdown.store(true, Ordering::Release);
                cv.notify_all();
            }
            for handle in db.compaction_handles.lock().drain(..) {
                handle.join().unwrap();
            }
            db.put(b"a", b"lower").unwrap();
            db.put(b"z", b"lower").unwrap();
            db.flush().unwrap();
            // A lower-level extent forces the later m compaction to retain
            // sequence numbers without containing an older value of m itself.
            {
                let mut inner = db.inner.lock();
                let file = inner.versions.current().level_files(0)[0].meta.clone();
                let mut edit = VersionEdit::new();
                edit.delete_file(0, file.number);
                edit.add_file(2, file);
                inner.versions.log_and_apply(edit).unwrap();
                inner.versions.sync_manifest().unwrap();
                db.install_super_version(&inner);
            }
            db.put(b"m", b"old").unwrap();
            db.flush().unwrap();
            let old_seq = db.read.current_sequence();
            let writer = db.clone();
            BEFORE_READ_VIEW.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    writer.put(b"m", b"new").unwrap();
                    writer.flush().unwrap();
                    writer.drain_l0(true).unwrap();
                    let version = writer.inner.lock().versions.current();
                    assert!(version.level_files(0).is_empty());
                    assert!(version.level_files(1).iter().all(|file| {
                        file.reader
                            .get_internal_with_seq(b"m", old_seq, false)
                            .unwrap()
                            .is_none()
                    }));
                }));
            });
            let options = ReadOptions::default();
            let value = match constructor {
                0 => db.get_with_options(&options, b"m").unwrap(),
                _ => {
                    let mut iter = match constructor {
                        1 => db
                            .iter_with_range(&options, Some(b"m"), Some(b"n"))
                            .unwrap(),
                        2 => db.iter_with_prefix(b"m", &options).unwrap(),
                        _ => db.iter_with_batch(&WriteBatchWithIndex::new()).unwrap(),
                    };
                    let value = iter.find(|(key, _)| key == b"m").map(|(_, value)| value);
                    assert!(iter.error().is_none());
                    value
                }
            };
            assert_eq!(
                value.as_deref(),
                Some(b"new".as_slice()),
                "constructor {constructor}"
            );
        }
    }

    #[test]
    fn dead_key_sweep_scheduler_preserves_requests_queued_while_running() {
        let scheduler = DeadKeySweepScheduler::new();

        assert!(!scheduler.try_start());
        scheduler.queue();
        assert!(scheduler.is_pending());
        assert!(scheduler.try_start());

        scheduler.queue();
        scheduler.finish(false);
        assert!(scheduler.try_start());

        scheduler.finish(false);
        assert!(!scheduler.is_pending());
        assert!(!scheduler.try_start());
    }

    #[test]
    fn dead_key_sweep_scheduler_retries_after_snapshot_release() {
        let scheduler = DeadKeySweepScheduler::new();

        assert!(!scheduler.retry_after_snapshot_release());
        scheduler.queue();
        assert!(scheduler.retry_after_snapshot_release());
        assert!(scheduler.try_start());

        assert!(scheduler.retry_after_snapshot_release());
        scheduler.finish(false);
        assert!(scheduler.try_start());

        scheduler.finish(true);
        assert!(scheduler.try_start());
        scheduler.finish(false);
    }

    /// `sync: true` must fsync the WAL before acknowledging, so a failed
    /// fsync reaches the writer; `sync: false` only flushes the WAL buffer.
    /// A crash test cannot observe this: the page cache survives it.
    #[test]
    fn sync_write_fsyncs_the_wal_and_async_write_does_not() {
        use crate::wal::writer::FAIL_NEXT_SYNC;

        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        FAIL_NEXT_SYNC.with(|fail| fail.set(true));
        let unsynced = db.put(b"a", b"1");
        let synced = db.put_with_options(
            &WriteOptions {
                durability: DurabilityMode::WalSync,
                ..Default::default()
            },
            b"b",
            b"2",
        );
        FAIL_NEXT_SYNC.with(|fail| fail.set(false));
        assert!(unsynced.is_ok(), "an async write must not fsync");
        assert!(synced.is_err(), "a sync write must fsync the WAL");
    }

    /// Join the compaction workers so a test drives compaction itself.
    fn stop_background_workers(db: &DB) {
        {
            let (lock, cv) = &*db.read.compaction_notify;
            let _guard = lock.lock();
            db.compaction_shutdown.store(true, Ordering::Release);
            cv.notify_all();
        }
        for handle in db.compaction_handles.lock().drain(..) {
            handle.join().unwrap();
        }
    }

    #[test]
    fn busy_l0_retry_requeues_eligible_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions::default()
            .with_l0_compaction_trigger(1)
            .with_l0_slowdown_trigger(1)
            .with_l0_stop_trigger(2);
        let db = DB::open(options, dir.path()).unwrap();
        stop_background_workers(&db);
        db.put(b"seed", b"initial").unwrap();
        db.flush_generation().unwrap();
        // Model a picker that consumed the flush wake while its L1 input
        // was claimed. The backlog is real and no other writer makes a wake.
        *db.read.compaction_notify.0.lock() = false;
        let no_wait = WriteOptions::default().with_no_slowdown(true);
        let err = db
            .put_with_options(&no_wait, b"retry", b"value")
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Busy);
        assert!(*db.read.compaction_notify.0.lock());
        db.drain_l0(true).unwrap();
        db.put_with_options(&no_wait, b"retry", b"value").unwrap();
    }

    #[test]
    fn repeated_reads_coalesce_pending_compaction_hints_by_level() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        stop_background_workers(&db);
        for (key, level) in [(b"a", 2), (b"m", 3)] {
            db.put(key, b"value").unwrap();
            db.flush().unwrap();
            let mut inner = db.inner.lock();
            let file = inner.versions.current().level_files(0)[0].meta.clone();
            let mut edit = VersionEdit::new();
            edit.delete_file(0, file.number);
            edit.add_file(level, file);
            inner.versions.log_and_apply(edit).unwrap();
            db.install_super_version(&inner);
        }
        // While a worker is occupied, repeated samples of the same levels
        // must retain only the work still needed, regardless of read volume.
        for _ in 0..64 {
            for key in [b"a", b"m"] {
                for _ in 0..1024 {
                    assert_eq!(db.get(key).unwrap().as_deref(), Some(b"value".as_slice()));
                }
            }
        }
        // Publish the samples after the last periodic poll before draining.
        db.read.check_read_compaction();
        let pending = mem::take(&mut *db.read.read_compaction_hints.lock());
        assert_eq!(
            pending.len(),
            2,
            "duplicate hints retained for the same levels"
        );
        assert!(pending.iter().any(|hint| hint.level == 2));
        assert!(pending.iter().any(|hint| hint.level == 3));
        for _ in 0..1024 {
            db.get(b"a").unwrap();
        }
        let pending = db.read.read_compaction_hints.lock();
        assert_eq!(pending.len(), 1, "new samples must requeue a drained level");
        assert_eq!(pending[0].level, 2);
    }

    /// `max_background_compactions: 0` is documented to mean one thread; two
    /// tests once relied on it to keep background compaction away.
    #[test]
    fn zero_background_compactions_still_starts_one_worker() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(
            DbOptions {
                max_background_compactions: 0,
                ..Default::default()
            },
            dir.path(),
        )
        .unwrap();
        assert_eq!(db.compaction_handles.lock().len(), 1);
    }

    /// Regression: a `bloom_bits_per_key` too large for even one key was
    /// accepted at open; writes were acknowledged and the first flush failed,
    /// fail-stopping the DB (and a reopen failed in the recovery flush).
    #[test]
    fn open_rejects_bloom_bits_no_flush_can_satisfy() {
        let dir = tempfile::tempdir().unwrap();
        let err = DB::open(
            DbOptions {
                bloom_bits_per_key: (crate::sst::META_BLOCK_HARD_LIMIT as u32) * 8 + 1,
                ..Default::default()
            },
            dir.path(),
        )
        .err()
        .expect("open must reject the filter size");
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    /// Regression: the open-time recovery flush built its own table options
    /// with `options.compression`, ignoring `compression_per_level[0]`.
    #[test]
    fn recovery_flush_uses_l0_compression() {
        use crate::sst::format::CompressionType;

        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            compression: CompressionType::Lz4,
            compression_per_level: vec![CompressionType::None; 7],
            ..Default::default()
        };
        let db = DB::open(options.clone(), dir.path()).unwrap();
        for i in 0..100 {
            db.put(format!("k{i:03}").as_bytes(), &[b'v'; 256]).unwrap();
        }
        db.simulate_crash();

        let db = DB::open(options, dir.path()).unwrap();
        let version = db.read.get_super_version().version.clone();
        let file = &version.level_files(0)[0];
        let first_block = file.reader.cached_index_entries().unwrap()[0].handle;
        let mut trailer_type = [0u8; 1];
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut f =
                fs::File::open(dir.path().join(format!("{:06}.sst", file.meta.number))).unwrap();
            f.seek(SeekFrom::Start(first_block.offset + first_block.size))
                .unwrap();
            f.read_exact(&mut trailer_type).unwrap();
        }
        assert_eq!(trailer_type[0], CompressionType::None as u8);
    }

    /// Regression: a WAL append failure reached group members as a string
    /// wrapped in `io::Error::other`, dropping the original `io::ErrorKind`
    /// (for example `StorageFull`) that `Error` exists to carry.
    #[test]
    fn wal_append_failure_keeps_the_io_error() {
        use std::error::Error as _;

        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        crate::wal::writer::FAIL_NEXT_APPEND
            .with(|fail| fail.set(Some(io::ErrorKind::StorageFull)));
        let err = db.put(b"k", b"v").unwrap_err();
        let io_kind = err
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .map(io::Error::kind);
        assert_eq!(io_kind, Some(io::ErrorKind::StorageFull), "{err}");
    }

    /// Regression: every DB iterator cloned each L1+ `TableFile` (a reader
    /// `Arc` bump plus two key copies) although the pinned `Version` already
    /// holds them for the iterator's lifetime.
    #[test]
    fn iterators_share_the_pinned_version_file_list() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        for i in 0..100 {
            db.put(format!("k{i:03}").as_bytes(), b"v").unwrap();
        }
        db.flush().unwrap();
        db.compact().unwrap();
        let version = db.read.get_super_version().version.clone();
        let reader = &version
            .level_files(1)
            .first()
            .expect("compact() must leave data in L1")
            .reader;
        let before = Arc::strong_count(reader);
        let iter = db.iter().unwrap();
        assert_eq!(
            Arc::strong_count(reader),
            before,
            "creating an iterator copied the L1 file list"
        );
        drop(iter);
    }

    /// Regression: flush cloned every memtable entry into a `Vec` before
    /// writing the first SST, holding a memtable-sized copy while both the
    /// frozen and the active memtable were resident.
    #[test]
    fn memtable_flush_streams_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mem = MemTable::new();
        for i in 0..4096u64 {
            mem.put(
                format!("key{i:06}").as_bytes(),
                &[b'v'; 4096],
                i + 1,
                ValueType::Value,
            );
        }
        let make_opts = || TableBuildOptions::default();
        let (outputs, allocations) = crate::test_alloc::measure(|| {
            DB::write_memtable_ssts(&mem, dir.path(), &make_opts, &FileNumbers::new(1), &[])
                .unwrap()
        });
        assert!(!outputs.is_empty());
        assert!(
            allocations.peak_live < 4 << 20,
            "flushing 16 MiB held {} bytes at once",
            allocations.peak_live
        );
    }

    #[test]
    fn flush_splits_small_memtable_for_large_bloom_filters() {
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            bloom_bits_per_key: ((META_BLOCK_SPLIT_THRESHOLD - 1) * 8) as u32,
            l0_compaction_trigger: usize::MAX,
            ..Default::default()
        };
        let db = DB::open(options, dir.path()).unwrap();
        for key in [b"a", b"b", b"c"] {
            db.put(key, key).unwrap();
        }
        db.flush().unwrap();
        assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("3"));
        db.close().unwrap();
        let reader = DB::open_read_only(dir.path()).unwrap();
        for key in [b"a", b"b", b"c"] {
            assert_eq!(reader.get(key).unwrap().as_deref(), Some(key.as_slice()));
        }
    }

    #[test]
    fn bloom_budget_below_split_threshold_splits_across_flush_recovery_ingest_and_compact() {
        // 1 key -> 33,553,408 bytes (< META_BLOCK_SPLIT_THRESHOLD = 33,554,432),
        // 2 keys -> 67,106,816 bytes (> META_BLOCK_HARD_LIMIT = 67,104,768).
        let bits_per_key = 268_427_264u32;
        assert!(
            BloomFilter::projected_size(1, bits_per_key) < META_BLOCK_SPLIT_THRESHOLD
                && BloomFilter::projected_size(2, bits_per_key) > crate::sst::META_BLOCK_HARD_LIMIT
        );

        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            num_levels: 3,
            prefix_len: 1,
            bloom_bits_per_key: bits_per_key,
            optimize_filters_for_hits: true,
            l0_compaction_trigger: usize::MAX,
            ..Default::default()
        };

        let db = DB::open(options.clone(), dir.path()).unwrap();
        // Exercise bottommost prefix-only filter split (`bloom_bits_for_level(2) == 0`,
        // `prefix_bloom_bits == bits_per_key`).
        assert_eq!(db.ingest_sorted([(b"x", b"vx"), (b"y", b"vy")]).unwrap(), 2);
        db.put(b"a", b"va").unwrap();
        db.put(b"b", b"vb").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("2"));
        db.compact().unwrap();

        let sync = WriteOptions::default().with_durability(DurabilityMode::WalSync);
        db.put_with_options(&sync, b"c", b"vc").unwrap();
        db.put_with_options(&sync, b"d", b"vd").unwrap();
        db.simulate_crash();

        let reopened = DB::open(options, dir.path()).unwrap();
        for (k, v) in [
            (b"a", b"va"),
            (b"b", b"vb"),
            (b"c", b"vc"),
            (b"d", b"vd"),
            (b"x", b"vx"),
            (b"y", b"vy"),
        ] {
            assert_eq!(reopened.get(k).unwrap().as_deref(), Some(v.as_slice()));
        }
        reopened.close().unwrap();
    }

    #[test]
    fn disabled_cache_skips_first_block_pin_io() {
        use crate::sst::table_reader::PREPARE_FIRST_BLOCK_PIN_HOOK;
        let probe_flush_pin_io = |options: DbOptions| -> bool {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open(options, dir.path()).unwrap();
            db.put(b"key", b"value").unwrap();
            let armed_on_worker = Arc::new(AtomicBool::new(false));
            let pin_io_ran = Arc::new(AtomicBool::new(false));
            let armed_clone = Arc::clone(&armed_on_worker);
            let pin_io_clone = Arc::clone(&pin_io_ran);
            db.flush_queue.on_next_build(move || {
                armed_clone.store(true, Ordering::SeqCst);
                PREPARE_FIRST_BLOCK_PIN_HOOK.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        pin_io_clone.store(true, Ordering::SeqCst);
                    }));
                });
            });
            db.flush().unwrap();
            assert!(
                armed_on_worker.load(Ordering::SeqCst),
                "flush hook must run on mmdb-flush worker"
            );
            pin_io_ran.load(Ordering::SeqCst)
        };

        assert!(
            probe_flush_pin_io(DbOptions::default()),
            "enabled cache control must exercise PREPARE_FIRST_BLOCK_PIN_HOOK on mmdb-flush"
        );
        for pool in [None, Some(Arc::new(crate::BlockCachePool::new(0)))] {
            assert!(
                !probe_flush_pin_io(DbOptions {
                    block_cache_capacity: 0,
                    block_cache: pool,
                    ..Default::default()
                }),
                "disabled cache read a block for pinning"
            );
        }
    }

    /// Regression: `drain_l0` looped on the full `pick_compaction`, so once L0
    /// was empty it ran level-size compactions inline — in the stop-trigger
    /// path while holding `write_queue`, stalling every writer.
    #[test]
    fn drain_l0_leaves_deeper_levels_to_background_workers() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(
            DbOptions {
                l0_compaction_trigger: usize::MAX,
                l0_slowdown_trigger: usize::MAX,
                l0_stop_trigger: usize::MAX,
                max_bytes_for_level_base: 1,
                ..Default::default()
            },
            dir.path(),
        )
        .unwrap();
        {
            let (lock, cv) = &*db.read.compaction_notify;
            let _guard = lock.lock();
            db.compaction_shutdown.store(true, Ordering::Release);
            cv.notify_all();
        }
        for handle in db.compaction_handles.lock().drain(..) {
            handle.join().unwrap();
        }
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();

        db.drain_l0(false).unwrap();
        let files_at = |level: usize| db.get_property(&format!("num-files-at-level{level}"));
        assert_eq!(files_at(0).as_deref(), Some("0"));
        assert_eq!(files_at(1).as_deref(), Some("1"));
        assert_eq!(
            files_at(2).as_deref(),
            Some("0"),
            "drain_l0 must not compact below L1"
        );
    }

    #[test]
    fn create_only_open_checks_current_under_directory_lock() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"key", b"value").unwrap();
        db.close().unwrap();
        let current = dir.path().join("CURRENT");
        let saved = dir.path().join("saved");
        fs::rename(&current, &saved).unwrap();
        // Model another opener finishing creation after our preflight.
        AFTER_DIRECTORY_LOCK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |_| fs::rename(saved, current).unwrap()));
        });
        let result = DB::open(
            DbOptions {
                error_if_exists: true,
                ..Default::default()
            },
            dir.path(),
        );
        assert_eq!(result.err().unwrap().kind(), ErrorKind::InvalidArgument);
        assert_eq!(
            DB::open_read_only(dir.path()).unwrap().get(b"key").unwrap(),
            Some(b"value".to_vec())
        );
    }

    /// Regression: open error paths closed the lock file without `LOCK_UN`.
    /// flock belongs to the open file description, so while another
    /// reference to it outlived the close, the failed open kept the lock and
    /// a same-process retry could not open the store.
    #[cfg(unix)]
    #[test]
    fn failed_open_releases_the_directory_lock() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let dir = tempfile::tempdir().unwrap();
        DB::open(DbOptions::default(), dir.path())
            .unwrap()
            .close()
            .unwrap();

        // A second reference to the lock's open file description, standing in
        // for a deferred final `fput` or a descriptor shared with a child.
        let shared: Rc<RefCell<Option<fs::File>>> = Rc::default();
        let slot = Rc::clone(&shared);
        AFTER_DIRECTORY_LOCK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |file: &fs::File| {
                *slot.borrow_mut() = Some(file.try_clone().unwrap());
            }));
        });
        FAIL_COMPACTION_SPAWN_AT.with(|f| f.set(Some(0)));
        let result = DB::open(DbOptions::default(), dir.path());
        FAIL_COMPACTION_SPAWN_AT.with(|f| f.set(None));
        assert!(result.is_err(), "the injected spawn failure must fail open");
        let shared_fd = shared.borrow_mut().take().expect("lock hook ran");

        let reopened = DB::open_read_only(dir.path());
        assert!(reopened.is_ok(), "{:?}", reopened.err());
        drop(shared_fd);
    }

    /// Regression: the background loops never re-checked `compaction_shutdown`,
    /// so `close()` joined workers only after every pending compaction ran.
    #[test]
    fn close_stops_background_compaction_after_the_current_job() {
        use std::sync::mpsc;
        use std::time::Instant;

        use crate::options::{CompactionFilter, CompactionFilterDecision};

        struct BlockFirstCall {
            armed: AtomicBool,
            entered: Mutex<Option<mpsc::Sender<()>>>,
            release: Mutex<Option<mpsc::Receiver<Arc<DB>>>>,
            close_result: mpsc::Sender<ErrorKind>,
        }
        impl CompactionFilter for BlockFirstCall {
            fn filter(
                &self,
                _level: usize,
                _key: &[u8],
                _value: &[u8],
            ) -> CompactionFilterDecision {
                if self.armed.swap(false, Ordering::SeqCst) {
                    self.entered.lock().take().unwrap().send(()).unwrap();
                    let db = self.release.lock().take().unwrap().recv().unwrap();
                    self.close_result
                        .send(db.close().unwrap_err().kind())
                        .unwrap();
                }
                CompactionFilterDecision::Keep
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let stalled = DbOptions {
            l0_compaction_trigger: usize::MAX,
            l0_slowdown_trigger: usize::MAX,
            l0_stop_trigger: usize::MAX,
            ..Default::default()
        };
        {
            let db = DB::open(stalled.clone(), dir.path()).unwrap();
            for file in 0..3 {
                for i in 0..50 {
                    db.put(format!("k{file}_{i:03}").as_bytes(), &[b'v'; 100])
                        .unwrap();
                }
                db.flush().unwrap();
            }
            db.close().unwrap();
        }

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (close_result, callback_done) = mpsc::channel();
        let filter = Arc::new(BlockFirstCall {
            armed: AtomicBool::new(true),
            entered: Mutex::new(Some(entered_tx)),
            release: Mutex::new(Some(release_rx)),
            close_result,
        });
        // The inherited L0 backlog starts one job; the tiny level budget
        // leaves more jobs queued behind it.
        let db = Arc::new(
            DB::open(
                DbOptions {
                    l0_compaction_trigger: 1,
                    max_bytes_for_level_base: 1,
                    target_file_size_base: 1024,
                    compaction_filter: Some(filter),
                    ..stalled
                },
                dir.path(),
            )
            .unwrap(),
        );
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("first compaction job never reached the filter");
        let completed_before = db.read.stats.compactions_completed.load(Ordering::Relaxed);

        let closer = {
            let db = Arc::clone(&db);
            thread::spawn(move || db.close())
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !db.compaction_shutdown.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "close never requested shutdown");
            thread::sleep(Duration::from_millis(1));
        }
        let holds_close_guard = db.lock_file.try_lock().is_none();
        let second = {
            let db = db.clone();
            thread::spawn(move || db.close())
        };
        release_tx.send(db.clone()).unwrap();
        assert_eq!(
            callback_done.recv_timeout(Duration::from_secs(10)).unwrap(),
            ErrorKind::InvalidArgument
        );
        second.join().unwrap().unwrap();
        let reopened = DB::open_read_only(dir.path()).unwrap();
        closer.join().unwrap().unwrap();
        assert!(
            holds_close_guard,
            "a concurrent close could return before LOCK was released"
        );
        drop(reopened);

        assert_eq!(
            db.read.stats.compactions_completed.load(Ordering::Relaxed),
            completed_before + 1,
            "close must stop after the in-flight job"
        );
    }

    /// Regression: range iterators applied the lower bound entry by entry
    /// instead of seeking to it, so the first `next()` walked every memtable
    /// and L0 entry below the bound. Reading below the bound is observable
    /// here: the only block below it is unreadable.
    #[test]
    fn range_iterator_seeks_to_lower_bound() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            block_size: 256,
            pin_l0_first_block_and_warm_index: false,
            l0_compaction_trigger: usize::MAX,
            l0_slowdown_trigger: usize::MAX,
            l0_stop_trigger: usize::MAX,
            ..Default::default()
        };
        let sst = {
            let db = DB::open(options.clone(), dir.path()).unwrap();
            for i in 0..100 {
                db.put(format!("a{i:03}").as_bytes(), &[b'v'; 64]).unwrap();
            }
            db.flush().unwrap();
            let number = db.inner.lock().versions.current().level_files(0)[0]
                .meta
                .number;
            db.close().unwrap();
            dir.path().join(format!("{number:06}.sst"))
        };
        let first_block = {
            let reader = crate::sst::table_reader::TableReader::open(&sst).unwrap();
            let index = reader.cached_index_entries().unwrap();
            assert!(index.len() > 2, "expected several data blocks");
            index[0].handle
        };
        let crc_byte_offset = first_block.offset + first_block.size + 1;
        let mut f = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sst)
            .unwrap();
        f.seek(SeekFrom::Start(crc_byte_offset)).unwrap();
        let mut byte = [0u8; 1];
        f.read_exact(&mut byte).unwrap();
        f.seek(SeekFrom::Start(crc_byte_offset)).unwrap();
        f.write_all(&[byte[0].wrapping_add(1)]).unwrap();
        drop(f);

        let db = DB::open(options, dir.path()).unwrap();
        let mut iter = db
            .iter_with_range(&ReadOptions::default(), Some(b"a050"), None)
            .unwrap();
        assert_eq!(iter.next().map(|(k, _)| k), Some(b"a050".to_vec()));
        assert!(iter.error().is_none(), "{:?}", iter.error());
    }

    /// Regression: `Path::exists()` maps every stat error to `false`, so an
    /// unreadable `CURRENT` made open create a fresh store and delete every
    /// SST of the existing one as orphans.
    #[cfg(unix)]
    #[test]
    fn open_fails_when_current_cannot_be_inspected() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = DB::open(DbOptions::default(), dir.path()).unwrap();
            db.put(b"k", b"v").unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }
        let count_ssts = || {
            fs::read_dir(dir.path())
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".sst")
                })
                .count()
        };
        let ssts = count_ssts();
        assert!(ssts > 0);
        let current = dir.path().join("CURRENT");
        let saved = fs::read(&current).unwrap();
        fs::remove_file(&current).unwrap();
        // A self-referential symlink makes stat fail with ELOOP.
        std::os::unix::fs::symlink("CURRENT", &current).unwrap();

        let check_errors = || {
            for result in [
                DB::open(DbOptions::default(), dir.path()),
                DB::open(
                    DbOptions {
                        create_if_missing: false,
                        ..Default::default()
                    },
                    dir.path(),
                ),
                DB::open_read_only(dir.path()),
            ] {
                assert_eq!(result.err().unwrap().kind(), ErrorKind::Io);
            }
        };
        check_errors();
        fs::remove_file(&current).unwrap();
        fs::create_dir(&current).unwrap();
        check_errors();
        fs::remove_dir(&current).unwrap();
        assert_eq!(count_ssts(), ssts, "a failed open must not delete SSTs");

        fs::write(&current, saved).unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    }

    /// Regression: block-property collectors are user code running inside the
    /// unlocked flush. A panic there skipped every fail-stop arm, so a later
    /// flush advanced `log_number` past the frozen memtable's WAL and its
    /// acknowledged writes were lost after reopen.
    #[test]
    fn collector_panic_during_flush_fail_stops() {
        use crate::options::BlockPropertyCollector;

        struct PanicOnBoom;
        impl BlockPropertyCollector for PanicOnBoom {
            fn add(&mut self, key: &[u8]) {
                if key == b"boom" {
                    panic!("collector rejected boom");
                }
            }
            fn finish_block(&mut self) -> Vec<u8> {
                Vec::new()
            }
            fn name(&self) -> &str {
                "panic-on-boom"
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            block_property_collectors: vec![Arc::new(|| {
                Box::new(PanicOnBoom) as Box<dyn BlockPropertyCollector>
            })],
            ..Default::default()
        };
        {
            let db = DB::open(options, dir.path()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"boom", b"2").unwrap();
            let flushed = catch_unwind(AssertUnwindSafe(|| db.flush()));
            assert!(
                matches!(flushed, Ok(Err(_))),
                "a collector panic must surface as a flush error"
            );
            assert!(db.put(b"c", b"3").is_err(), "engine must fail-stop");
        }
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(db.get(b"boom").unwrap().as_deref(), Some(&b"2"[..]));
    }

    /// Regression: a flush can allocate its L0 file numbers and install
    /// them after an unlocked write. A sweep that rewrote L0 in between gave
    /// older data higher file numbers, so after reopen (L0 sorted by number)
    /// a point read returned the overwritten value.
    #[test]
    fn dead_key_sweep_defers_l0_while_flush_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            l0_compaction_trigger: usize::MAX,
            l0_slowdown_trigger: usize::MAX,
            l0_stop_trigger: usize::MAX,
            ..Default::default()
        };
        {
            let db = DB::open(options.clone(), dir.path()).unwrap();
            // This test drives the sweep step itself.
            {
                let (lock, cv) = &*db.read.compaction_notify;
                let _guard = lock.lock();
                db.compaction_shutdown.store(true, Ordering::Release);
                cv.notify_all();
            }
            for handle in db.compaction_handles.lock().drain(..) {
                handle.join().unwrap();
            }
            db.put(b"k", b"v1").unwrap();
            db.flush().unwrap();
            db.put(b"x", b"x").unwrap();
            db.flush().unwrap();
            db.put(b"k", b"v2").unwrap();

            let frozen = db.freeze_memtable().unwrap();
            let snapshots = Vec::new();
            let ctx = CompactionContext {
                db_path: &db.storage_path,
                dir_sync: &db.dir_sync,
                manifest_poisoned: &db.read.manifest_poisoned,
                options: &db.options,
                builds: &db.builds,
                rate_limiter: Some(&db.rate_limiter),
                stats: Some(&db.read.stats),
                active_snapshots: &snapshots,
            };
            {
                let swept = db.level_rewrite().run(0, &ctx, false).unwrap();
                assert!(!swept, "L0 sweep must wait for the in-flight flush");
            }
            db.flush_context().flush(&frozen).unwrap();
            assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
            db.close().unwrap();
        }
        let db = DB::open(options, dir.path()).unwrap();
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
    }

    /// Regression: the explicit flush only woke a sweep already marked
    /// Pending. A pass that deferred L0 for this flush but had not yet called
    /// `finish` was still Running, so no wake was sent and the sweep then
    /// stayed Pending with nothing left to trigger it.
    #[test]
    fn flush_install_wakes_a_sweep_still_deferring_l0() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        stop_background_workers(&db);
        db.put(b"k", b"v").unwrap();
        let frozen = db.freeze_memtable().unwrap();
        // The pass has seen the in-flight flush and skipped L0.
        db.dead_key_sweep.queue();
        assert!(db.dead_key_sweep.try_start());
        *db.read.compaction_notify.0.lock() = false;

        db.flush_context().flush(&frozen).unwrap();
        db.dead_key_sweep.finish(true);

        assert!(db.dead_key_sweep.is_pending());
        assert!(
            *db.read.compaction_notify.0.lock(),
            "the deferred sweep must have a queued wake"
        );
    }

    /// A second worker can consume the flush notification while the sweep
    /// owner is still Running. The owner must retry after publishing Pending
    /// if the flush completed, or wait for the flush if it is still in flight.
    #[test]
    fn deferred_sweep_rechecks_flush_after_another_worker_consumes_its_wakeup() {
        use std::sync::mpsc;
        use std::time::Instant;

        for flush_before_finish in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open(
                DbOptions {
                    max_background_compactions: 2,
                    l0_compaction_trigger: usize::MAX,
                    l0_slowdown_trigger: usize::MAX,
                    l0_stop_trigger: usize::MAX,
                    lazy_delete_compaction_threshold: 1,
                    ..Default::default()
                },
                dir.path(),
            )
            .unwrap();
            db.put(b"k", b"v").unwrap();
            let frozen = db.freeze_memtable().unwrap();
            let complete_flush = || {
                db.flush_context().flush(&frozen).unwrap();
            };
            let wait_for_passes = |count| {
                let deadline = Instant::now() + Duration::from_secs(10);
                while db.dead_key_sweep.completed_passes.load(Ordering::Acquire) < count {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                true
            };
            // Open unconditionally schedules a startup pass. Let it finish
            // before counting the two passes used by this interleaving.
            assert!(wait_for_passes(1), "startup compaction pass did not finish");

            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            *db.dead_key_sweep.before_deferred_finish.lock() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }));
            db.lazy_delete(b"k");
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();

            if flush_before_finish {
                complete_flush();
                // Worker A is parked before finish; only worker B can complete
                // this pass, consuming the notification without starting a sweep.
                let consumed = wait_for_passes(2);
                let wake_queued = *db.read.compaction_notify.0.lock();
                release_tx.send(()).unwrap();
                assert!(consumed, "second worker did not consume the notification");
                assert!(!wake_queued, "notification was not consumed before finish");
            } else {
                release_tx.send(()).unwrap();
                assert!(wait_for_passes(2), "sweep did not defer the pending flush");
                assert!(db.dead_key_sweep.is_pending());
                assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
                complete_flush();
            }

            assert!(wait_for_passes(3), "deferred sweep did not complete");
            assert_eq!(
                db.get(b"k").unwrap(),
                None,
                "cleanup lost its wakeup (flush_before_finish={flush_before_finish})"
            );
            db.close().unwrap();
        }
    }

    /// Regression: `prune_settled_dead_keys`'s absence probe is an unlocked
    /// scan, so a concurrent write can commit a new version of a key
    /// between "observed absent" and "registration removed". Deterministically
    /// land a write to a registered dead key inside exactly that window (via
    /// `PRUNE_SETTLED_RACE_HOOK`, which fires once the probe has already
    /// decided the key is settled) and confirm the registration survives —
    /// so a later compaction still enforces the documented "reused while
    /// still registered" contract instead of the race silently exempting
    /// the rewrite from ever being checked again.
    #[test]
    fn prune_settled_dead_keys_reverifies_under_write_queue_before_removing() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());

        // Registered while genuinely absent (never written), so the probe's
        // very first pass finds it settled.
        db.lazy_delete(b"race_key");
        assert_eq!(db.dead_key_count(), 1);

        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (written_tx, written_rx) = std::sync::mpsc::channel::<()>();

        // Arm the hook: it fires once, exactly between the probe (which will
        // have already decided "race_key" is settled) and the
        // re-verify-and-remove step. It wakes the writer below and then
        // blocks until that write has fully committed, deterministically
        // placing the concurrent rewrite inside the race window under test.
        PRUNE_SETTLED_RACE_HOOK.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || {
                started_tx.send(()).unwrap();
                written_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }));
        });

        thread::scope(|scope| {
            let writer_db = &db;
            scope.spawn(move || {
                started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                writer_db
                    .put(b"race_key", b"rewritten-while-active")
                    .unwrap();
                written_tx.send(()).unwrap();
            });

            db.prune_settled_dead_keys(None, None, None);
        });

        assert_eq!(
            db.dead_key_count(),
            1,
            "a write racing the absence probe must keep the registration active"
        );
        assert_eq!(
            db.get(b"race_key").unwrap(),
            Some(b"rewritten-while-active".to_vec()),
            "the concurrent write itself must not be lost"
        );

        // Per the documented contract, a value written while its
        // registration was still active is removed by a later compaction.
        // `compact()` only rewrites SST files, so flush the memtable first.
        db.flush().unwrap();
        db.compact().unwrap();
        assert_eq!(
            db.get(b"race_key").unwrap(),
            None,
            "a later compaction must still remove a value written while lazy \
             deletion was active"
        );
        assert_eq!(db.dead_key_count(), 0);
    }

    /// Regression: the re-verify phase must not hold `write_queue` across
    /// every settled key. Each re-verify is a real lookup (memtables + every
    /// L0 file + one file per lower level), so a single critical section over
    /// an uncapped settled list — which explicit `compact`/`compact_range`
    /// produce — blocks every writer for that whole scan. Assert the locked
    /// phase is chunked, and that chunking did not weaken the removal
    /// contract for keys that stayed absent.
    #[test]
    fn prune_settled_dead_keys_chunks_the_write_queue_barrier() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());

        // Enough settled keys to require several chunks.
        let count = DEAD_KEY_RECONFIRM_CHUNK * 4 + 7;
        for i in 0..count {
            db.lazy_delete(format!("gone{i:06}").as_bytes());
        }
        assert_eq!(db.dead_key_count(), count);

        PRUNE_SETTLED_LOCK_COUNT.with(|c| c.set(0));
        db.prune_settled_dead_keys(None, None, None);
        let locks = PRUNE_SETTLED_LOCK_COUNT.with(|c| c.get());

        let expected = count.div_ceil(DEAD_KEY_RECONFIRM_CHUNK);
        assert_eq!(
            locks, expected,
            "re-verify must take write_queue once per chunk of \
             {DEAD_KEY_RECONFIRM_CHUNK}, not once for all {count} keys"
        );
        assert!(
            locks > 1,
            "a single acquisition would hold the writer barrier across every key"
        );
        assert_eq!(
            db.dead_key_count(),
            0,
            "every still-absent registration must still be pruned"
        );
    }

    #[test]
    fn impossible_worker_count_fails_before_database_creation() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            max_background_compactions: usize::MAX,
            ..Default::default()
        };
        let result = DB::open(opts.clone(), dir.path());
        assert_eq!(result.err().unwrap().kind(), ErrorKind::InvalidArgument);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
        open_test_db(dir.path()).close().unwrap();
        DB::open_read_only_with_options(opts, dir.path()).unwrap();
    }

    /// Regression: if a later worker's spawn fails during `DB::open`, the
    /// workers spawned before it must not be left running forever. Force
    /// worker 2 (of 5) to fail via `FAIL_COMPACTION_SPAWN_AT` and confirm
    /// every worker — including workers 0 and 1, which `DB::open` must
    /// signal and join before returning the error — actually exits, via
    /// `Arc::strong_count` on the shared shutdown flag dropping back to
    /// just this test's own reference.
    #[test]
    fn db_open_partial_compaction_spawn_failure_joins_prior_workers() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            max_background_compactions: 5,
            ..Default::default()
        };

        FAIL_COMPACTION_SPAWN_AT.with(|f| f.set(Some(2)));
        let result = DB::open(opts, dir.path());
        FAIL_COMPACTION_SPAWN_AT.with(|f| f.set(None));

        assert!(
            result.is_err(),
            "the injected spawn failure must propagate from DB::open"
        );

        let shutdown_flag = LAST_COMPACTION_SHUTDOWN_FLAG
            .with(|sink| sink.borrow_mut().take())
            .expect("DB::open must deposit its compaction_shutdown flag");

        // Workers 0 and 1 must already be joined by the time `DB::open`
        // returns; worker 2's real (detached) thread only needs to notice
        // the same shutdown signal and exit, which the poll below allows a
        // short bounded time for.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let count = Arc::strong_count(&shutdown_flag);
            if count == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "compaction workers spawned before the injected failure were \
                 not joined/exited (Arc::strong_count = {count})"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// L0 first-block reads run outside the DB mutex in the flush worker.
    #[test]
    fn flush_first_block_pin_read_does_not_hold_inner_lock() {
        use crate::sst::table_reader::PREPARE_FIRST_BLOCK_PIN_HOOK;

        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"k1", &[b'x'; 4096]).unwrap();

        thread::scope(|scope| {
            let db_ref = &db;
            let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
            let (checked_tx, checked_rx) = std::sync::mpsc::channel::<bool>();

            // Arm the worker's thread-local reader hook before it begins the build.
            db.flush_queue.on_next_build(move || {
                PREPARE_FIRST_BLOCK_PIN_HOOK.with(|h| {
                    *h.borrow_mut() = Some(Box::new(move || {
                        reached_tx.send(()).unwrap();
                        let acquired = checked_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                        assert!(
                            acquired,
                            "a direct try_lock on `inner` failed while the first-block \
                         pin read was in flight — the lock must not be held during \
                         that I/O"
                        );
                    }));
                })
            });

            scope.spawn(move || {
                reached_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                // A direct try_lock on `inner` — bypassing the
                // write-queue/group-commit protocol entirely (so it is not
                // confounded by group-commit's own, unrelated, "queued
                // writers wait for the current leader" serialization) —
                // must succeed immediately: nothing, including this
                // flush, may be holding `inner` while the first-block pin
                // read is in flight.
                let acquired = db_ref.inner.try_lock().is_some();
                let _ = checked_tx.send(acquired);
            });

            db.flush().unwrap();
        });
    }

    fn open_test_db(dir: &Path) -> DB {
        let opts = DbOptions {
            create_if_missing: true,
            ..Default::default()
        };
        DB::open(opts, dir).unwrap()
    }

    fn open_test_db_small_memtable(dir: &Path) -> DB {
        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 1024,
            ..Default::default()
        };
        DB::open(opts, dir).unwrap()
    }

    #[test]
    fn test_open_create() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        assert!(db.path().exists());
        assert!(dir.path().join("CURRENT").exists());
    }

    #[test]
    fn test_open_create_if_missing_false_rejects_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        assert!(path.exists());
        assert!(!path.join("CURRENT").exists());

        let err = match DB::open(
            DbOptions {
                create_if_missing: false,
                ..Default::default()
            },
            path,
        ) {
            Ok(_) => panic!("empty dir must not create a DB when create_if_missing is false"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
        assert!(
            !path.join("CURRENT").exists(),
            "must not create CURRENT when create_if_missing is false"
        );
        assert!(
            !path.join("LOCK").exists(),
            "must not create LOCK when open fails before initialization"
        );
        assert!(
            std::fs::read_dir(path).unwrap().next().is_none(),
            "empty dir must stay without DB files"
        );
    }

    #[test]
    fn test_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"hello", b"world").unwrap();
        assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(db.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn test_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
        db.delete(b"key").unwrap();
        assert_eq!(db.get(b"key").unwrap(), None);
    }

    #[test]
    fn test_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"key", b"v1").unwrap();
        db.put(b"key", b"v2").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_write_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        let mut batch = WriteBatch::new();
        batch.put(b"k1", b"v1");
        batch.put(b"k2", b"v2");
        batch.delete(b"k3");
        db.write(batch).unwrap();
        assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_snapshot_read() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"key", b"v1").unwrap();
        let snap = db.snapshot();
        db.put(b"key", b"v2").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"v2".to_vec()));
        let opts = ReadOptions {
            snapshot: Some((&snap).into()),
            ..Default::default()
        };
        assert_eq!(
            db.get_with_options(&opts, b"key").unwrap(),
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn test_close() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"key", b"val").unwrap();
        db.close().unwrap();
        assert!(db.put(b"key2", b"val2").is_err());
    }

    #[test]
    fn test_close_releases_resources_before_drop() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"key", b"val").unwrap();
        db.close().unwrap();

        assert!(db.compaction_shutdown.load(Ordering::Acquire));
        assert!(db.compaction_handles.lock().is_empty());
        assert!(db.lock_file.lock().is_none());

        let reopened = open_test_db(dir.path());
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"val".to_vec()));
    }

    #[test]
    fn close_releases_file_handles_and_preserves_properties_and_iterators() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
        let reader = Arc::downgrade(&db.inner.lock().versions.current().level_files(0)[0].reader);
        let names = [
            "total-sst-size",
            "num-files-at-level0",
            "compaction-pending",
        ];
        let properties = names.map(|name| db.get_property(name));
        let mut iter = db.iter().unwrap();
        db.close().unwrap();
        assert_eq!(names.map(|name| db.get_property(name)), properties);
        assert!(db.inner.lock().wal_writer.is_none());
        assert!(
            db.inner
                .lock()
                .versions
                .manifest_sync_handle()
                .lock()
                .is_none()
        );
        assert_eq!(iter.next(), Some((b"key".to_vec(), b"value".to_vec())));
        drop(iter);
        assert!(reader.upgrade().is_none());
    }

    #[test]
    fn test_close_reports_existing_background_error() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.set_bg_error("simulated auto-flush failure".to_string());

        let err = db.close().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Background);
        assert_eq!(err.message(), "simulated auto-flush failure");
        assert!(db.compaction_handles.lock().is_empty());
        assert!(db.lock_file.lock().is_none());
    }

    #[test]
    fn test_close_waits_out_concurrent_auto_flush() {
        // Proves close cannot release LOCK while a group-commit leader is still
        // mid auto-flush: after close returns, a reopen must succeed without
        // "already locked", and previously acknowledged keys must survive.
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(open_test_db_small_memtable(dir.path()));
        let stop = Arc::new(AtomicBool::new(false));
        let mut writers = Vec::new();
        for t in 0..4u8 {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            writers.push(thread::spawn(move || {
                let mut i = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("t{t}-{:06}", i);
                    let val = vec![b'v'; 64];
                    if db.put(key.as_bytes(), &val).is_err() {
                        break;
                    }
                    i = i.wrapping_add(1);
                }
            }));
        }
        thread::sleep(Duration::from_millis(50));
        db.close().expect("close during auto-flush");
        stop.store(true, Ordering::Relaxed);
        for h in writers {
            h.join().unwrap();
        }
        assert!(db.lock_file.lock().is_none());
        let reopened = open_test_db(dir.path());
        // At least some writer progress must have been durable through the
        // close barrier (empty DB would mean writers never landed, which makes
        // the race test uninteresting).
        let mut it = reopened.iter().unwrap();
        assert!(
            it.next().is_some(),
            "expected durable keys after concurrent close"
        );
    }

    /// A parked auto-flush lets the writer return while close waits for durability.
    #[test]
    fn close_waits_for_flush_after_the_writer_returns() {
        use crate::options::BlockPropertyCollector;

        struct FlushGate {
            entered: Arc<Mutex<Option<std::sync::mpsc::Sender<()>>>>,
            release: Arc<Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
        }
        impl BlockPropertyCollector for FlushGate {
            fn add(&mut self, key: &[u8]) {
                if key != b"gate" {
                    return;
                }
                let Some(release) = self.release.lock().take() else {
                    return;
                };
                if let Some(entered) = self.entered.lock().take() {
                    let _ = entered.send(());
                }
                let _ = release.recv_timeout(Duration::from_secs(10));
            }
            fn finish_block(&mut self) -> Vec<u8> {
                Vec::new()
            }
            fn name(&self) -> &str {
                "close-flush-gate"
            }
        }

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let entered = Arc::new(Mutex::new(Some(entered_tx)));
        let release = Arc::new(Mutex::new(Some(release_rx)));
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            DB::open(
                DbOptions {
                    create_if_missing: true,
                    write_buffer_size: 1024,
                    block_property_collectors: vec![Arc::new({
                        let entered = Arc::clone(&entered);
                        let release = Arc::clone(&release);
                        move || {
                            Box::new(FlushGate {
                                entered: Arc::clone(&entered),
                                release: Arc::clone(&release),
                            }) as Box<dyn BlockPropertyCollector>
                        }
                    })],
                    ..Default::default()
                },
                dir.path(),
            )
            .unwrap(),
        );
        stop_background_workers(&db);

        let writer = Arc::clone(&db);
        let (put_tx, put_rx) = std::sync::mpsc::channel();
        let put = thread::spawn(move || {
            writer.put(b"gate", &[b'v'; 2048]).unwrap();
            put_tx.send(()).unwrap();
        });
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("flush worker did not enter collector");
        put_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("WAL writer stalled behind SST build");
        let close_done = Arc::new(AtomicBool::new(false));
        let close_flag = Arc::clone(&close_done);
        let closing = Arc::clone(&db);
        let close = thread::spawn(move || {
            closing.close().unwrap();
            close_flag.store(true, Ordering::Release);
        });
        thread::sleep(Duration::from_millis(200));
        assert!(
            !close_done.load(Ordering::Acquire),
            "close returned while auto-flush still held the group-commit leader"
        );

        release_tx.send(()).unwrap();
        put.join().unwrap();
        close.join().unwrap();
        assert!(close_done.load(Ordering::Acquire));
        assert!(db.lock_file.lock().is_none());

        let reopened = open_test_db(dir.path());
        assert_eq!(
            reopened.get(b"gate").unwrap().as_deref(),
            Some([b'v'; 2048].as_slice())
        );
    }

    #[test]
    fn test_close_serializes_with_compact_range() {
        // compact_range holds write_queue across flush+install. Concurrent close
        // must either wait for that barrier before detaching LOCK or reject the
        // range job via check_usable — never both detach and let install finish.
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 256,
            ..Default::default()
        };
        let db = Arc::new(DB::open(opts, dir.path()).unwrap());
        // Keep background out of the way so the range job does the install.
        stop_background_workers(&db);
        for i in 0..40u32 {
            let k = format!("cr{:04}", i);
            db.put(k.as_bytes(), &[b'v'; 32]).unwrap();
            if i % 5 == 4 {
                db.flush().unwrap();
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::new();
        for t in 0..3u8 {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            workers.push(thread::spawn(move || {
                let mut i = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    let begin = format!("cr{:04}", (i * 3) % 40);
                    let end = format!("cr{:04}", ((i * 3) + 8).min(40));
                    let _ = db.compact_range(Some(begin.as_bytes()), Some(end.as_bytes()));
                    let key = format!("w{t}-{:05}", i);
                    let _ = db.put(key.as_bytes(), b"x");
                    i = i.wrapping_add(1);
                }
            }));
        }
        thread::sleep(Duration::from_millis(40));
        db.close().expect("close during compact_range");
        stop.store(true, Ordering::Relaxed);
        for h in workers {
            h.join().unwrap();
        }
        assert!(db.lock_file.lock().is_none());
        // Reopen must not hit "already locked" from a post-close installer.
        let reopened = DB::open(
            DbOptions {
                create_if_missing: false,
                ..Default::default()
            },
            dir.path(),
        )
        .expect("reopen after concurrent compact_range close");
        assert_eq!(
            reopened.get(b"cr0000").unwrap(),
            Some(vec![b'v'; 32]),
            "pre-close data must remain readable"
        );
    }

    #[test]
    fn l0_stop_trigger_waits_while_compaction_holds_l0_claim() {
        // A compaction filter parks the in-flight L0 merge. The stop trigger
        // must not admit another write until that claim drops; otherwise
        // flushes keep adding L0 files for the whole compaction.
        struct GateFilter {
            entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
            release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        }
        impl CompactionFilter for GateFilter {
            fn filter(&self, _: usize, _: &[u8], _: &[u8]) -> CompactionFilterDecision {
                if let Some(release) = self.release.lock().take() {
                    if let Some(entered) = self.entered.lock().take() {
                        let _ = entered.send(());
                    }
                    let _ = release.recv_timeout(Duration::from_secs(10));
                }
                CompactionFilterDecision::Keep
            }
            fn is_noop(&self) -> bool {
                false
            }
        }

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            DB::open(
                DbOptions {
                    create_if_missing: true,
                    write_buffer_size: 256,
                    l0_compaction_trigger: 1,
                    l0_slowdown_trigger: 1,
                    l0_stop_trigger: 1,
                    compaction_filter: Some(Arc::new(GateFilter {
                        entered: Mutex::new(Some(entered_tx)),
                        release: Mutex::new(Some(release_rx)),
                    })),
                    ..Default::default()
                },
                dir.path(),
            )
            .unwrap(),
        );

        // no_slowdown fails fast once L0 hits the trigger, so setup cannot
        // block inside the stop-trigger drain while the filter is parked.
        let setup = WriteOptions {
            no_slowdown: true,
            ..Default::default()
        };
        for i in 0u32..16 {
            let _ = db.put_with_options(&setup, &i.to_be_bytes(), &[b'x'; 64]);
        }
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("background compaction never reached the filter");
        assert!(
            db.l0_file_count.load(Ordering::Relaxed) >= 1,
            "stop trigger must observe the claimed L0 file"
        );

        let writer = Arc::clone(&db);
        let finished = Arc::new(AtomicBool::new(false));
        let finished_flag = Arc::clone(&finished);
        let handle = thread::spawn(move || {
            writer.put(b"stop-key", b"v").unwrap();
            finished_flag.store(true, Ordering::Release);
        });
        thread::sleep(Duration::from_millis(300));
        assert!(
            !finished.load(Ordering::Acquire),
            "write returned while an L0 compaction still held the stop-trigger claim"
        );
        release_tx.send(()).unwrap();
        handle.join().unwrap();
        assert!(finished.load(Ordering::Acquire));
        assert_eq!(
            db.get(b"stop-key").unwrap().as_deref(),
            Some(b"v".as_slice())
        );
    }

    #[test]
    fn snapshot_during_compaction_filter_keeps_removed_value() {
        // The filter parks, a snapshot is registered, then the filter removes
        // the key. Install must discard that output so the snapshot still
        // sees the pre-filter value.
        struct RemoveAfterGate {
            target: Vec<u8>,
            entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
            release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        }
        impl CompactionFilter for RemoveAfterGate {
            fn filter(&self, _: usize, key: &[u8], _: &[u8]) -> CompactionFilterDecision {
                if key != self.target.as_slice() {
                    return CompactionFilterDecision::Keep;
                }
                if let Some(release) = self.release.lock().take() {
                    if let Some(entered) = self.entered.lock().take() {
                        let _ = entered.send(());
                    }
                    let _ = release.recv_timeout(Duration::from_secs(10));
                }
                CompactionFilterDecision::Remove
            }
            fn is_noop(&self) -> bool {
                false
            }
        }

        let target = 0u32.to_be_bytes();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            DB::open(
                DbOptions {
                    create_if_missing: true,
                    write_buffer_size: 256,
                    l0_compaction_trigger: 1,
                    compaction_filter: Some(Arc::new(RemoveAfterGate {
                        target: target.to_vec(),
                        entered: Mutex::new(Some(entered_tx)),
                        release: Mutex::new(Some(release_rx)),
                    })),
                    ..Default::default()
                },
                dir.path(),
            )
            .unwrap(),
        );

        db.put(&target, b"visible").unwrap();
        for i in 1u32..8 {
            db.put(&i.to_be_bytes(), &[b'x'; 64]).unwrap();
        }
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("background compaction never reached the filter");

        let snap = db.snapshot();
        assert_eq!(
            db.get_with_options(&snap.read_options(), &target)
                .unwrap()
                .as_deref(),
            Some(b"visible".as_slice())
        );

        let compacting = Arc::clone(&db);
        let compact = thread::spawn(move || compacting.compact());
        thread::sleep(Duration::from_millis(50));
        release_tx.send(()).unwrap();
        compact.join().unwrap().unwrap();

        assert_eq!(
            db.get_with_options(&snap.read_options(), &target)
                .unwrap()
                .as_deref(),
            Some(b"visible".as_slice()),
            "snapshot taken during compaction must still see the pre-filter value"
        );
    }

    #[test]
    fn test_close_serializes_with_l0_stop_drain() {
        // Stop-trigger drain ran before becoming group leader and was outside
        // the close barrier. Race writers intoL0 stop while closing.
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 128,
            l0_compaction_trigger: 2,
            l0_slowdown_trigger: 2,
            l0_stop_trigger: 3,
            ..Default::default()
        };
        let db = Arc::new(DB::open(opts, dir.path()).unwrap());
        // Only the writers' inline stop-trigger drains may relieve L0.
        stop_background_workers(&db);
        let stop = Arc::new(AtomicBool::new(false));
        let mut writers = Vec::new();
        for t in 0..4u8 {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            writers.push(thread::spawn(move || {
                let mut i = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("st{t}-{:06}", i);
                    if db.put(key.as_bytes(), &[b'x'; 64]).is_err() {
                        break;
                    }
                    i = i.wrapping_add(1);
                }
            }));
        }
        thread::sleep(Duration::from_millis(60));
        db.close().expect("close during L0 stop drain");
        stop.store(true, Ordering::Relaxed);
        for h in writers {
            h.join().unwrap();
        }
        assert!(db.lock_file.lock().is_none());
        let _reopened = DB::open(
            DbOptions {
                create_if_missing: false,
                ..Default::default()
            },
            dir.path(),
        )
        .expect("reopen after concurrent stop-drain close");
    }

    #[test]
    fn test_flush_serializes_with_auto_flush_leader_without_wal() {
        // disable_wal: a flush that returns while an auto-flush install is still
        // in flight would acknowledge durability of data that exists only in the
        // immutable memtable. Concurrent puts + repeated flush exercise the
        // leader barrier; after writers stop, one final flush + crash must keep
        // every acknowledged key.
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            ..Default::default()
        };
        let db = Arc::new(DB::open(opts, dir.path()).unwrap());
        let wo = WriteOptions {
            durability: DurabilityMode::DisableWal,
            ..Default::default()
        };
        let stop = Arc::new(AtomicBool::new(false));
        let written = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let mut writers = Vec::new();
        for t in 0..3u8 {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let written = Arc::clone(&written);
            let wo = wo.clone();
            writers.push(thread::spawn(move || {
                let mut i = 0u32;
                while !stop.load(Ordering::Relaxed) && i < 300 {
                    let key = format!("nw{t}-{:05}", i).into_bytes();
                    if db.put_with_options(&wo, &key, b"x").is_err() {
                        break;
                    }
                    written.lock().push(key);
                    i = i.wrapping_add(1);
                }
            }));
        }
        for _ in 0..8 {
            db.flush()
                .expect("flush during concurrent disable_wal auto-flush");
            thread::sleep(Duration::from_millis(2));
        }
        stop.store(true, Ordering::Relaxed);
        for h in writers {
            h.join().unwrap();
        }
        let keys = written.lock().clone();
        assert!(!keys.is_empty());
        db.flush().expect("final flush after writers stop");
        let db = Arc::try_unwrap(db).unwrap_or_else(|_| panic!("writers still hold db"));
        db.simulate_crash();
        let reopened = DB::open(
            DbOptions {
                create_if_missing: false,
                ..Default::default()
            },
            dir.path(),
        )
        .unwrap();
        for k in &keys {
            assert_eq!(
                reopened.get(k).unwrap(),
                Some(b"x".to_vec()),
                "key {} lost after flush+crash without WAL",
                String::from_utf8_lossy(k)
            );
        }
    }

    #[test]
    fn test_close_reports_poisoned_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.read.manifest_poisoned.store(true, Ordering::Release);

        let err = db.close().unwrap_err();
        // A failed MANIFEST write fail-stops a recoverable store; it is not
        // on-disk corruption (callers quarantine on `Corruption`).
        assert_eq!(err.kind(), ErrorKind::Background);
        assert!(err.message().contains("MANIFEST writer poisoned"));
        assert!(db.compaction_handles.lock().is_empty());
        assert!(db.lock_file.lock().is_none());
    }

    #[test]
    fn test_directory_lock_prevents_double_open() {
        let dir = tempfile::tempdir().unwrap();
        let _db = open_test_db(dir.path());
        // Attempting to open the same directory again should fail
        let result = DB::open(
            DbOptions {
                create_if_missing: true,
                ..Default::default()
            },
            dir.path(),
        );
        assert!(result.is_err(), "double-open should fail with lock error");
    }

    #[test]
    fn test_many_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        for i in 0u32..1000 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        for i in 0u32..1000 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
    }

    #[test]
    fn test_concurrent_reads_writes() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(open_test_db(dir.path()));
        for i in 0..100 {
            db.put(
                format!("key_{}", i).as_bytes(),
                format!("val_{}", i).as_bytes(),
            )
            .unwrap();
        }

        let mut handles = vec![];
        for t in 0..4 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("thread_{}_key_{}", t, i);
                    let val = format!("thread_{}_val_{}", t, i);
                    db.put(key.as_bytes(), val.as_bytes()).unwrap();
                }
            }));
        }
        for _ in 0..4 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let _ = db.get(format!("key_{}", i).as_bytes());
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        for t in 0..4 {
            for i in 0..100 {
                let key = format!("thread_{}_key_{}", t, i);
                let val = format!("thread_{}_val_{}", t, i);
                assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
            }
        }
    }

    #[test]
    fn test_flush_and_read_from_sst() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());

        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let val = format!("value_{}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();

        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let val = format!("value_{}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "failed reading key {} after flush",
                i
            );
        }

        db.put(b"new_key", b"new_val").unwrap();
        assert_eq!(db.get(b"new_key").unwrap(), Some(b"new_val".to_vec()));
    }

    #[test]
    fn initial_manifest_without_a_wal_can_be_opened() {
        let dir = tempfile::tempdir().unwrap();
        // Simulate interruption between publishing the first CURRENT and
        // installing the first WAL number. Zero is the bootstrap sentinel.
        drop(VersionSet::create(dir.path(), DbOptions::default().num_levels).unwrap());
        let reader = DB::open_read_only(dir.path()).unwrap();
        assert!(reader.get(b"k").unwrap().is_none());
        reader.close().unwrap();
        let writer = DB::open(DbOptions::default(), dir.path()).unwrap();
        writer.put(b"k", b"v").unwrap();
        writer.sync_wal().unwrap();
        writer.simulate_crash();
        let reader = DB::open_read_only(dir.path()).unwrap();
        assert_eq!(reader.get(b"k").unwrap().as_deref(), Some(b"v".as_slice()));
    }

    #[test]
    fn test_wal_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();

        {
            let db = open_test_db(&dir_path);
            db.put(b"survive_key", b"survive_val").unwrap();
            db.put(b"another_key", b"another_val").unwrap();
            if let Some(ref mut wal) = db.inner.lock().wal_writer {
                wal.sync().unwrap();
            }
            db.read.closed.store(true, Ordering::Release);
        }

        {
            let db = open_test_db(&dir_path);
            assert_eq!(
                db.get(b"survive_key").unwrap(),
                Some(b"survive_val".to_vec())
            );
            assert_eq!(
                db.get(b"another_key").unwrap(),
                Some(b"another_val".to_vec())
            );
        }
    }

    #[test]
    fn simulated_crash_releases_readers_and_discards_pending_wal_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        db.put(b"key", b"value").unwrap();
        let state = Arc::downgrade(&db.inner);
        let wal_path = dir
            .path()
            .join(format!("{:06}.wal", db.inner.lock().wal_number));
        let committed = fs::read(&wal_path).unwrap();
        db.inner
            .lock()
            .wal_writer
            .as_mut()
            .unwrap()
            .add_record(b"pending")
            .unwrap();
        db.simulate_crash();
        assert!(state.upgrade().is_none());
        assert_eq!(fs::read(wal_path).unwrap(), committed);
        assert_eq!(
            open_test_db(dir.path()).get(b"key").unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn recovery_preserves_a_wal_with_trailing_batch_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());
        let mut batch = WriteBatch::new();
        batch.put(b"key", b"value");
        let mut record = [batch.wal_header(1).as_slice(), &batch.payload].concat();
        record.push(0);
        let wal_path = {
            let mut inner = db.inner.lock();
            let wal = inner.wal_writer.as_mut().unwrap();
            wal.add_record(&record).unwrap();
            wal.sync().unwrap();
            dir.path().join(format!("{:06}.wal", inner.wal_number))
        };
        db.simulate_crash();
        let before = fs::read(&wal_path).unwrap();
        for result in [
            DB::open_read_only(dir.path()),
            DB::open(DbOptions::default(), dir.path()),
        ] {
            assert_eq!(result.err().unwrap().kind(), ErrorKind::Corruption);
            assert_eq!(fs::read(&wal_path).unwrap(), before);
        }
    }

    #[test]
    fn test_recovery_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();

        {
            let db = open_test_db(&dir_path);
            for i in 0..20 {
                let key = format!("flushed_{:04}", i);
                let val = format!("value_{}", i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            db.flush().unwrap();

            for i in 20..30 {
                let key = format!("flushed_{:04}", i);
                let val = format!("value_{}", i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            if let Some(ref mut wal) = db.inner.lock().wal_writer {
                wal.sync().unwrap();
            }
            db.read.closed.store(true, Ordering::Release);
        }

        {
            let db = open_test_db(&dir_path);
            for i in 0..30 {
                let key = format!("flushed_{:04}", i);
                let val = format!("value_{}", i);
                assert_eq!(
                    db.get(key.as_bytes()).unwrap(),
                    Some(val.into_bytes()),
                    "failed at key {} after recovery",
                    i
                );
            }
        }
    }

    #[test]
    fn test_small_memtable_auto_flush() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db_small_memtable(dir.path());

        for i in 0..100 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:060}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let sst_count = fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".sst")
            })
            .count();
        assert!(sst_count > 0, "should have flushed to SST");

        for i in 0..100 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:060}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
    }

    #[test]
    fn test_db_iterator() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());

        db.put(b"c", b"3").unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();

        let entries: Vec<_> = db.iter().unwrap().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(entries[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(entries[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_db_iterator_with_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());

        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        db.delete(b"b").unwrap();

        let entries: Vec<_> = db.iter().unwrap().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(entries[1], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_db_iterator_across_flush() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());

        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();
        db.put(b"c", b"3").unwrap();
        db.put(b"a", b"updated").unwrap();

        let entries: Vec<_> = db.iter().unwrap().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (b"a".to_vec(), b"updated".to_vec()));
        assert_eq!(entries[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(entries[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_db_iterator_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());

        db.put(b"a", b"v1").unwrap();
        let snap = db.snapshot();
        db.put(b"a", b"v2").unwrap();
        db.put(b"b", b"new").unwrap();

        // Iterator at snapshot should see old value and no "b"
        let opts = ReadOptions {
            snapshot: Some((&snap).into()),
            ..Default::default()
        };
        let entries: Vec<_> = db.iter_with_options(&opts).unwrap().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (b"a".to_vec(), b"v1".to_vec()));
    }

    #[test]
    fn test_db_iterator_seek() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_test_db(dir.path());

        for i in 0..10 {
            let key = format!("key_{:02}", i);
            let val = format!("val_{}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let mut iter = db.iter().unwrap();
        iter.seek(b"key_05");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"key_05");

        let remaining: Vec<_> = iter.collect();
        assert_eq!(remaining.len(), 5); // key_05 through key_09
    }

    #[test]
    fn test_lz4_compression() {
        use crate::sst::format::CompressionType;

        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            compression: CompressionType::Lz4,
            write_buffer_size: 512,
            ..Default::default()
        };
        let db = DB::open(opts, dir.path()).unwrap();

        for i in 0..100 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:060}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify all data is readable (after auto-flush with LZ4)
        for i in 0..100 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:060}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
    }

    #[test]
    fn test_preset_profiles() {
        let dir1 = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::write_heavy(), dir1.path()).unwrap();
        db.put(b"test", b"value").unwrap();
        assert_eq!(db.get(b"test").unwrap(), Some(b"value".to_vec()));

        let dir2 = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::read_heavy(), dir2.path()).unwrap();
        db.put(b"test", b"value").unwrap();
        assert_eq!(db.get(b"test").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn monkey_level_bloom_budgets_and_optimize_filters_for_hits() {
        // 1. Monkey per-level schedule (`num_levels = 3`, `b = 12`, `R = 10`):
        //    L0 -> 22 bpk, L1 -> 17 bpk, L2 (bottom) -> 11 bpk.
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions::default()
            .with_num_levels(3)
            .with_bloom_bits_per_key(12)
            .with_l0_compaction_trigger(usize::MAX);
        assert_eq!(opts.bloom_bits_for_level(0), 22);
        assert_eq!(opts.bloom_bits_for_level(1), 17);
        assert_eq!(opts.bloom_bits_for_level(2), 11);

        let db = DB::open(opts, dir.path()).unwrap();
        for i in 0..1000u32 {
            db.put(format!("p1_{i:04}").as_bytes(), b"v1").unwrap();
        }
        db.flush().unwrap();
        {
            let v = db.inner.lock().versions.current();
            assert_eq!(v.level_files(0).len(), 1);
            assert_eq!(
                v.level_files(0)[0].reader.bloom_filter_byte_len(),
                Some(BloomFilter::projected_size(1000, 22))
            );
        }

        for i in 0..1000u32 {
            db.put(format!("p2_{i:04}").as_bytes(), b"v2").unwrap();
        }
        db.flush().unwrap();
        db.drain_l0(true).unwrap();
        {
            let v = db.inner.lock().versions.current();
            assert_eq!(v.level_files(0).len(), 0);
            assert_eq!(v.level_files(1).len(), 1);
            assert_eq!(
                v.level_files(1)[0].reader.bloom_filter_byte_len(),
                Some(BloomFilter::projected_size(2000, 17))
            );
        }

        db.compact_range(Some(b""), None).unwrap();
        {
            let v = db.inner.lock().versions.current();
            assert_eq!(v.level_files(2).len(), 1);
            assert_eq!(
                v.level_files(2)[0].reader.bloom_filter_byte_len(),
                Some(BloomFilter::projected_size(2000, 11))
            );
        }

        // 2. `optimize_filters_for_hits(true)` omits `filter.bloom` on bottommost
        //    SSTs while still building `filter.prefix` when `prefix_len > 0`.
        let dir2 = tempfile::tempdir().unwrap();
        let hit_opts = DbOptions::default()
            .with_num_levels(2)
            .with_bloom_bits_per_key(12)
            .with_prefix_len(3)
            .with_optimize_filters_for_hits(true)
            .with_l0_compaction_trigger(usize::MAX);
        assert_eq!(hit_opts.bloom_bits_for_level(0), 17);
        assert_eq!(hit_opts.bloom_bits_for_level(1), 0);

        let db2 = DB::open(hit_opts, dir2.path()).unwrap();
        for i in 0..200u32 {
            db2.put(format!("aa_{i:04}").as_bytes(), b"va").unwrap();
        }
        db2.flush().unwrap();
        for i in 0..200u32 {
            db2.put(format!("bb_{i:04}").as_bytes(), b"vb").unwrap();
        }
        db2.flush().unwrap();
        {
            let v = db2.inner.lock().versions.current();
            assert_eq!(v.level_files(0).len(), 2);
            assert!(v.level_files(0)[0].reader.bloom_filter_byte_len().is_some());
        }

        db2.compact_range(None, None).unwrap();
        {
            let v = db2.inner.lock().versions.current();
            assert_eq!(v.level_files(1).len(), 1);
            let bottom_reader = &v.level_files(1)[0].reader;
            assert_eq!(
                bottom_reader.bloom_filter_byte_len(),
                None,
                "bottommost SST must omit point filter.bloom when optimize_filters_for_hits=true"
            );
            assert!(bottom_reader.prefix_may_match(b"aa_"));
            assert!(
                !bottom_reader.prefix_may_match(b"zz_"),
                "bottommost SST must still retain prefix Bloom filter"
            );
        }

        assert_eq!(
            db2.get(b"aa_0042").unwrap().as_deref(),
            Some(b"va".as_slice())
        );
        assert_eq!(db2.get(b"missing_key").unwrap(), None);
        assert!(db2.contains_key(b"bb_0199").unwrap());
        assert!(!db2.contains_key(b"missing_key").unwrap());
        let aa_count = db2
            .iter_with_prefix(b"aa_", &ReadOptions::default())
            .unwrap()
            .count();
        assert_eq!(aa_count, 200);
    }

    #[test]
    fn recovery_rejects_malformed_wal_batch_headers_and_entries() {
        // 1. Valid post-ingest_sorted active WAL (where MANIFEST last_sequence > first WAL seq)
        //    must still recover cleanly.
        {
            let dir = tempfile::tempdir().unwrap();
            let db = open_test_db(dir.path());
            db.put_with_options(
                &WriteOptions::default().with_durability(DurabilityMode::WalSync),
                b"m",
                b"1",
            )
            .unwrap();
            assert_eq!(db.ingest_sorted([(b"z", b"2")]).unwrap(), 1);
            db.put_with_options(
                &WriteOptions::default().with_durability(DurabilityMode::WalSync),
                b"n",
                b"3",
            )
            .unwrap();
            db.simulate_crash();

            let reopened = open_test_db(dir.path());
            assert_eq!(reopened.get(b"m").unwrap(), Some(b"1".to_vec()));
            assert_eq!(reopened.get(b"z").unwrap(), Some(b"2".to_vec()));
            assert_eq!(reopened.get(b"n").unwrap(), Some(b"3".to_vec()));
        }

        // 2. Malformed WAL batch records fail closed with ErrorKind::Corruption
        //    in both open_read_only and writable open while preserving WAL bytes.
        let mut dup_b1 = WriteBatch::new();
        dup_b1.put(b"k", b"v1");
        let mut dup_b2 = WriteBatch::new();
        dup_b2.put(b"k", b"v2");
        let dup_records = vec![
            [dup_b1.wal_header(1).as_slice(), &dup_b1.payload].concat(),
            [dup_b2.wal_header(1).as_slice(), &dup_b2.payload].concat(),
        ];

        let mut zero_count = Vec::new();
        zero_count.extend_from_slice(&1u64.to_le_bytes());
        zero_count.extend_from_slice(&0u32.to_le_bytes());

        let mut zero_seq = Vec::new();
        zero_seq.extend_from_slice(&0u64.to_le_bytes());
        zero_seq.extend_from_slice(&1u32.to_le_bytes());
        zero_seq.push(ValueType::Deletion as u8);
        zero_seq.extend_from_slice(&1u32.to_le_bytes());
        zero_seq.push(b'k');

        let mut inverted_range = Vec::new();
        inverted_range.extend_from_slice(&1u64.to_le_bytes());
        inverted_range.extend_from_slice(&1u32.to_le_bytes());
        inverted_range.push(ValueType::RangeDeletion as u8);
        inverted_range.extend_from_slice(&1u32.to_le_bytes());
        inverted_range.push(b'z');
        inverted_range.extend_from_slice(&1u32.to_le_bytes());
        inverted_range.push(b'a');

        let big_key_len = (MAX_USER_KEY_SIZE + 1) as u32;
        let mut oversized_key = Vec::with_capacity(17 + MAX_USER_KEY_SIZE + 1);
        oversized_key.extend_from_slice(&1u64.to_le_bytes());
        oversized_key.extend_from_slice(&1u32.to_le_bytes());
        oversized_key.push(ValueType::Deletion as u8);
        oversized_key.extend_from_slice(&big_key_len.to_le_bytes());
        oversized_key.resize(oversized_key.len() + big_key_len as usize, b'x');

        for records in [
            dup_records,
            vec![zero_count],
            vec![zero_seq],
            vec![inverted_range],
            vec![oversized_key],
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = open_test_db(dir.path());
            let wal_path = {
                let mut inner = db.inner.lock();
                let wal = inner.wal_writer.as_mut().unwrap();
                for rec in &records {
                    wal.add_record(rec).unwrap();
                }
                wal.sync().unwrap();
                dir.path().join(format!("{:06}.wal", inner.wal_number))
            };
            db.simulate_crash();
            let before = fs::read(&wal_path).unwrap();
            for result in [
                DB::open_read_only(dir.path()),
                DB::open(DbOptions::default(), dir.path()),
            ] {
                assert_eq!(result.err().unwrap().kind(), ErrorKind::Corruption);
                assert_eq!(fs::read(&wal_path).unwrap(), before);
            }
        }
    }

    #[test]
    fn flush_deduplicates_overwrites_while_preserving_snapshot_and_rangedel_visibility() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(
            DbOptions::default()
                .with_write_buffer_size(64 << 20)
                .with_l0_compaction_trigger(usize::MAX),
            dir.path(),
        )
        .unwrap();

        // 1. Without snapshots, repeated overwrites coalesce in the flushed L0 SST.
        for round in 0..16u8 {
            for k in 0..64u32 {
                db.put(&format!("hot_{k:04}").into_bytes(), &[round; 256])
                    .unwrap();
            }
        }
        // Snapshot boundaries + interleaved range deletion must remain exact after flush.
        db.put(b"snap_k", b"v1").unwrap();
        db.put(b"ranged_k", b"rv1").unwrap();
        let s1 = db.snapshot();

        db.put(b"snap_k", b"v2").unwrap();
        db.delete_range(b"ranged_a", b"ranged_z").unwrap();
        let s2 = db.snapshot();

        // Multiple overwrites inside the same `(s2, latest]` stripe still deduplicate
        // to `v4`, while `s1` and `s2` keep `v1` and `v2`.
        db.put(b"snap_k", b"v3").unwrap();
        db.put(b"snap_k", b"v4").unwrap();
        db.put(b"ranged_k", b"rv2").unwrap();

        db.flush().unwrap();

        let sst_bytes: u64 = db.get_property("total-sst-size").unwrap().parse().unwrap();
        // 64 keys * 256B = ~16 KiB deduplicated vs > 260 KiB un-deduplicated.
        assert!(
            sst_bytes < 64 * 1024,
            "unexpected flushed L0 size: {sst_bytes}"
        );

        for k in 0..64u32 {
            assert_eq!(
                db.get(&format!("hot_{k:04}").into_bytes())
                    .unwrap()
                    .as_deref(),
                Some([15u8; 256].as_slice())
            );
        }
        assert_eq!(
            s1.get(b"snap_k").unwrap().as_deref(),
            Some(b"v1".as_slice())
        );
        assert_eq!(
            s2.get(b"snap_k").unwrap().as_deref(),
            Some(b"v2".as_slice())
        );
        assert_eq!(
            db.get(b"snap_k").unwrap().as_deref(),
            Some(b"v4".as_slice())
        );

        assert_eq!(
            s1.get(b"ranged_k").unwrap().as_deref(),
            Some(b"rv1".as_slice())
        );
        assert_eq!(s2.get(b"ranged_k").unwrap(), None);
        assert_eq!(
            db.get(b"ranged_k").unwrap().as_deref(),
            Some(b"rv2".as_slice())
        );
    }

    #[test]
    fn hinted_trivial_move_drains_newly_over_budget_target_level_and_respects_filters() {
        struct RewriteCountingFilter(Arc<AtomicUsize>);
        impl CompactionFilter for RewriteCountingFilter {
            fn filter(&self, _lvl: usize, _k: &[u8], _v: &[u8]) -> CompactionFilterDecision {
                self.0.fetch_add(1, Ordering::Relaxed);
                CompactionFilterDecision::Keep
            }
        }

        // 1. Five-level layout: two under-budget L2 files + L3 just below budget.
        //    A single read hint on L2 moves one file to L3, pushing L3 over budget;
        //    the worker must automatically drain L3 -> L4 without another wakeup!
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(
            DbOptions::default()
                .with_num_levels(5)
                .with_compression(crate::CompressionType::None)
                .with_max_bytes_for_level_base(20_000)
                .with_max_bytes_for_level_multiplier(1.0), // L1=20KB, L2=20KB, L3=20KB
            dir.path(),
        )
        .unwrap();

        let build_sst = |path: &Path, num: u64, lo: &[u8], hi: &[u8], val_len: usize| {
            let sst = path.join(format!("{num:06}.sst"));
            let mut b = TableBuilder::new(
                &sst,
                TableBuildOptions {
                    compression: crate::CompressionType::None,
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            let v = vec![b'p'; val_len];
            b.add(
                types::InternalKey::new(lo, 10, ValueType::Value).as_bytes(),
                &v,
            )
            .unwrap();
            b.add(
                types::InternalKey::new(hi, 10, ValueType::Value).as_bytes(),
                &v,
            )
            .unwrap();
            flush::file_metadata((num, b.finish().unwrap()))
        };

        {
            let synced = sync_new_sst_parent_dir(&db.dir_sync, &db.read.manifest_poisoned).unwrap();
            let mut inner = db.inner.lock();
            let f1 = inner.versions.file_numbers.allocate().unwrap();
            let f2 = inner.versions.file_numbers.allocate().unwrap();
            let f3 = inner.versions.file_numbers.allocate().unwrap();
            let mut edit = VersionEdit::new();
            // L2 total = ~13 KB (< 20 KB budget), 2 disjoint files (~6.5 KB each)
            edit.add_file(2, build_sst(&db.storage_path, f1, b"a0", b"a9", 3_000));
            edit.add_file(2, build_sst(&db.storage_path, f2, b"c0", b"c9", 3_000));
            // L3 total = ~17 KB (< 20 KB budget); moving 1 L2 file pushes L3 > 20 KB!
            edit.add_file(3, build_sst(&db.storage_path, f3, b"m0", b"m9", 8_000));
            edit.set_next_file_number(inner.versions.next_file_number());
            inner.versions.log_and_apply_synced(edit, synced).unwrap();
            inner.versions.sync_manifest().unwrap();
            db.install_super_version(&inner);
        }
        assert_eq!(db.get_property("compaction-pending").as_deref(), Some("0"));

        // Push one L2 hint and signal the worker ONCE.
        db.read
            .read_compaction_hints
            .lock()
            .push(CompactionHint { level: 2 });
        db.read.signal_compaction();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if db.get_property("compaction-pending").as_deref() == Some("0")
                && db.get_property("num-files-at-level4").as_deref() != Some("0")
            {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(db.get_property("compaction-pending").as_deref(), Some("0"));
        assert_eq!(db.get_property("num-files-at-level2").as_deref(), Some("1"));
        assert!(
            db.get_property("num-files-at-level4")
                .unwrap()
                .parse::<usize>()
                .unwrap()
                >= 1
        );

        // 2. Under-budget L2 with an active `CompactionFilter` must NOT rewrite SSTs on hint.
        let dir2 = tempfile::tempdir().unwrap();
        let filter_calls = Arc::new(AtomicUsize::new(0));
        let db2 = DB::open(
            DbOptions::default()
                .with_num_levels(4)
                .with_compression(crate::CompressionType::None)
                .with_max_bytes_for_level_base(50_000)
                .with_max_bytes_for_level_multiplier(1.0)
                .with_compaction_filter(Some(Arc::new(RewriteCountingFilter(Arc::clone(
                    &filter_calls,
                ))))),
            dir2.path(),
        )
        .unwrap();
        {
            let synced =
                sync_new_sst_parent_dir(&db2.dir_sync, &db2.read.manifest_poisoned).unwrap();
            let mut inner = db2.inner.lock();
            let f1 = inner.versions.file_numbers.allocate().unwrap();
            let f2 = inner.versions.file_numbers.allocate().unwrap();
            let mut edit = VersionEdit::new();
            edit.add_file(2, build_sst(&db2.storage_path, f1, b"a0", b"a9", 2_000));
            edit.add_file(2, build_sst(&db2.storage_path, f2, b"c0", b"c9", 2_000));
            edit.set_next_file_number(inner.versions.next_file_number());
            inner.versions.log_and_apply_synced(edit, synced).unwrap();
            inner.versions.sync_manifest().unwrap();
            db2.install_super_version(&inner);
        }
        db2.read
            .read_compaction_hints
            .lock()
            .push(CompactionHint { level: 2 });
        db2.read.signal_compaction();
        thread::sleep(Duration::from_millis(80));
        assert_eq!(filter_calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            db2.get_property("stats.compaction_bytes_written")
                .as_deref(),
            Some("0")
        );
        assert_eq!(
            db2.get_property("num-files-at-level2").as_deref(),
            Some("2")
        );

        // 3. Under-budget L2 where a range tombstone pulls multiple source files
        //    (`input_files_level.len() > 1`) is NOT a trivial move and must also skip rewrite.
        let dir3 = tempfile::tempdir().unwrap();
        let db3 = DB::open(
            DbOptions::default()
                .with_num_levels(4)
                .with_compression(crate::CompressionType::None)
                .with_max_bytes_for_level_base(50_000)
                .with_max_bytes_for_level_multiplier(1.0),
            dir3.path(),
        )
        .unwrap();
        {
            let synced =
                sync_new_sst_parent_dir(&db3.dir_sync, &db3.read.manifest_poisoned).unwrap();
            let mut inner = db3.inner.lock();
            let f1 = inner.versions.file_numbers.allocate().unwrap();
            let f2 = inner.versions.file_numbers.allocate().unwrap();
            let sst1 = db3.storage_path.join(format!("{f1:06}.sst"));
            let mut b1 = TableBuilder::new(
                &sst1,
                TableBuildOptions {
                    compression: crate::CompressionType::None,
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            b1.add(
                types::InternalKey::new(b"a0", 11, ValueType::RangeDeletion).as_bytes(),
                b"z9",
            )
            .unwrap();
            b1.add(
                types::InternalKey::new(b"a0", 10, ValueType::Value).as_bytes(),
                &[b'p'; 1_000],
            )
            .unwrap();
            let m1 = flush::file_metadata((f1, b1.finish().unwrap()));
            let m2 = build_sst(&db3.storage_path, f2, b"c0", b"c9", 1_000);
            let mut edit = VersionEdit::new();
            edit.add_file(2, m1);
            edit.add_file(2, m2);
            edit.set_next_file_number(inner.versions.next_file_number());
            inner.versions.log_and_apply_synced(edit, synced).unwrap();
            inner.versions.sync_manifest().unwrap();
            db3.install_super_version(&inner);
        }
        db3.read
            .read_compaction_hints
            .lock()
            .push(CompactionHint { level: 2 });
        db3.read.signal_compaction();
        thread::sleep(Duration::from_millis(80));
        assert_eq!(
            db3.get_property("stats.compaction_bytes_written")
                .as_deref(),
            Some("0")
        );
        assert_eq!(
            db3.get_property("num-files-at-level2").as_deref(),
            Some("2")
        );
    }

    #[cfg(feature = "bench-metrics")]
    #[test]
    fn write_profile_counts_actual_parks_not_spin_completions() {
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(open_test_db(dir.path()));

        // 1. Spin-only completion: hold `write_queue` with `leader_active = true` briefly,
        //    let follower enqueue and spin, then complete its request immediately before 384 spins.
        let db_spin = Arc::clone(&db);
        let (enq_tx, enq_rx) = std::sync::mpsc::channel();
        let follower = thread::spawn(move || {
            let _ = crate::take_thread_write_profile();
            enq_tx.send(()).unwrap();
            db_spin.put(b"spin_k", b"v").unwrap();
            crate::take_thread_write_profile()
        });
        enq_rx.recv().unwrap();
        // Wait until follower pushes into queue while we simulate an active leader:
        // easier & race-free: acquire `write_barrier`, `claim_leadership`, drop `barrier`,
        // then once `wq.queue` is non-empty, drain and complete!
        let _ = follower.join().unwrap(); // warm call

        let mut barrier = db.acquire_write_barrier();
        barrier.claim_leadership();
        drop(barrier);
        let db_spin2 = Arc::clone(&db);
        let spin_handle = thread::spawn(move || {
            let _ = crate::take_thread_write_profile();
            db_spin2.put(b"spin_k2", b"v2").unwrap();
            crate::take_thread_write_profile()
        });
        let spin_deadline = Instant::now() + Duration::from_secs(5);
        while db.write_queue.lock().queue.is_empty() {
            assert!(
                Instant::now() < spin_deadline,
                "timed out waiting for spin follower to enqueue"
            );
            std::hint::spin_loop();
        }
        // Complete immediately while follower is still in its spin window (or check WalSync repeated park)
        let group = db
            .write_queue
            .lock()
            .drain_group(db.options.write_buffer_size);
        let mut dummy = false;
        let res = db.write_batch_group(&group, std::ptr::null_mut(), &mut dummy);
        let ok = res.map(|_| ());
        DB::complete_write_group(&group, std::ptr::null_mut(), &ok);
        db.release_write_leadership();
        let spin_prof = spin_handle.join().unwrap();
        assert_eq!(spin_prof.queue_waits, 1);
        if spin_prof.queue_wakeups == 0 {
            assert_eq!(spin_prof.queue_wait_ns, 0);
        }

        // 2. Repeated park (`WalSync` has `spin_limit = 0`, so follower parks immediately):
        //    wake the head with `STATE_WAKE` while `leader_active` stays true and wait
        //    until it clears `STATE_WAKE` and re-sets `STATE_PARKED` before completing.
        let mut barrier = db.acquire_write_barrier();
        barrier.claim_leadership();
        drop(barrier);
        let db_park = Arc::clone(&db);
        let park_handle = thread::spawn(move || {
            let _ = crate::take_thread_write_profile();
            db_park
                .put_with_options(
                    &WriteOptions::default().with_durability(DurabilityMode::WalSync),
                    b"park_k",
                    b"v",
                )
                .unwrap();
            crate::take_thread_write_profile()
        });
        let park_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let wq = db.write_queue.lock();
            if let Some(&rp) = wq.queue.front() {
                // SAFETY: `wq` is held while `leader_active` prevents draining or
                // completing `rp`, so the parked follower's stack `WriteRequest` is live.
                let s = unsafe { (*rp).state.load(Ordering::Acquire) };
                if (s & STATE_PARKED) != 0 {
                    break;
                }
            }
            drop(wq);
            assert!(
                Instant::now() < park_deadline,
                "timed out waiting for follower initial park"
            );
            thread::yield_now();
        }
        // Ensure non-zero elapsed wait time across coarse timers, then issue a
        // leader-blocked `STATE_WAKE` handoff so the follower wakes from park #1,
        // clears `STATE_WAKE | STATE_PARKED` under `write_queue`, and parks a second time.
        thread::sleep(Duration::from_millis(1));
        let wake_thread = {
            let wq = db.write_queue.lock();
            DB::wake_queue_head(&wq)
        };
        wake_thread.expect("parked head waiter").unpark();
        let repark_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let wq = db.write_queue.lock();
            if let Some(&rp) = wq.queue.front() {
                // SAFETY: `wq` is held while `leader_active` prevents draining or
                // completing `rp`, so the follower's stack `WriteRequest` stays live.
                let s = unsafe { (*rp).state.load(Ordering::Acquire) };
                if (s & (STATE_WAKE | STATE_PARKED)) == STATE_PARKED {
                    break;
                }
            }
            drop(wq);
            assert!(
                Instant::now() < repark_deadline,
                "timed out waiting for follower second park"
            );
            thread::yield_now();
        }
        let group = db
            .write_queue
            .lock()
            .drain_group(db.options.write_buffer_size);
        let mut dummy = false;
        let res = db.write_batch_group(&group, std::ptr::null_mut(), &mut dummy);
        let ok = res.map(|_| ());
        DB::complete_write_group(&group, std::ptr::null_mut(), &ok);
        db.release_write_leadership();
        let park_prof = park_handle.join().unwrap();
        assert_eq!(park_prof.queue_waits, 1);
        assert!(
            park_prof.queue_wakeups >= 2,
            "expected >= 2 park returns, got {}",
            park_prof.queue_wakeups
        );
        assert!(park_prof.queue_wait_ns > 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn hint_wal_writeback_skips_out_of_range_i64_bounds() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("writeback.wal");
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.write_all(&[0x5A; 64]).unwrap();
        file.flush().unwrap();

        super::hint_wal_writeback(&file, 0, 64);
        super::hint_wal_writeback(&file, (i64::MAX as u64) + 1, 64);
        super::hint_wal_writeback(&file, 0, (i64::MAX as u64) + 1);
        super::hint_wal_writeback(&file, u64::MAX, u64::MAX);
    }

    #[test]
    fn read_hint_filter_is_noop_runs_under_callback_guard() {
        use std::sync::{Weak, mpsc};

        struct HintProbeFilter {
            store: Arc<Mutex<Weak<DB>>>,
            checked: Mutex<Option<mpsc::Sender<()>>>,
        }
        impl CompactionFilter for HintProbeFilter {
            fn filter(&self, _: usize, _: &[u8], _: &[u8]) -> CompactionFilterDecision {
                CompactionFilterDecision::Keep
            }
            fn is_noop(&self) -> bool {
                if let Some(tx) = self.checked.lock().take() {
                    let db = self.store.lock().upgrade().expect("live DB handle");
                    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"v".as_slice()));
                    for attempt in [
                        db.put(b"recursive", b"bad"),
                        db.flush(),
                        db.sync_wal(),
                        db.compact_range(None, None),
                        db.compact(),
                        db.close(),
                    ] {
                        assert_eq!(attempt.unwrap_err().kind(), ErrorKind::InvalidArgument);
                    }
                    tx.send(()).unwrap();
                }
                true
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Mutex::new(Weak::new()));
        let (tx, rx) = mpsc::channel();
        let db = Arc::new(
            DB::open(
                DbOptions::default()
                    .with_num_levels(3)
                    .with_compaction_filter(Some(Arc::new(HintProbeFilter {
                        store: Arc::clone(&store),
                        checked: Mutex::new(Some(tx)),
                    }))),
                dir.path(),
            )
            .unwrap(),
        );
        *store.lock() = Arc::downgrade(&db);

        assert_eq!(db.ingest_sorted([(b"k", b"v")]).unwrap(), 1);
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"v".as_slice()));
        rx.recv_timeout(Duration::from_secs(5))
            .expect("read-hint worker never invoked CompactionFilter::is_noop");
        assert_eq!(db.get(b"recursive").unwrap(), None);
        db.close().unwrap();
    }

    #[test]
    fn parallel_subcompaction_callback_releasing_final_db_handle_avoids_deadlock() {
        use std::sync::{Weak, mpsc};

        struct SubcompactionDropFilter {
            store: Arc<Mutex<Weak<DB>>>,
            upgraded_tx: Mutex<Option<mpsc::Sender<()>>>,
            release_rx: Mutex<Option<mpsc::Receiver<()>>>,
        }
        impl CompactionFilter for SubcompactionDropFilter {
            fn filter(
                &self,
                target_level: usize,
                key: &[u8],
                _: &[u8],
            ) -> CompactionFilterDecision {
                if target_level == 2
                    && let Some(rx) = self.release_rx.lock().take()
                {
                    let db = self.store.lock().upgrade().expect("live DB handle");
                    assert!(db.get(key).unwrap().is_some());
                    self.upgraded_tx.lock().take().unwrap().send(()).unwrap();
                    rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    // Dropping `db` here runs `DB::drop` on a scoped subcompaction
                    // worker while the parent compaction worker waits in `thread::scope`.
                    drop(db);
                }
                CompactionFilterDecision::Keep
            }
            fn is_noop(&self) -> bool {
                false
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Mutex::new(Weak::new()));
        let (upgraded_tx, upgraded_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let opts = DbOptions::default()
            .with_num_levels(3)
            .with_l0_compaction_trigger(1)
            .with_max_subcompactions(2)
            .with_max_bytes_for_level_base(128)
            .with_compaction_filter(Some(Arc::new(SubcompactionDropFilter {
                store: Arc::clone(&store),
                upgraded_tx: Mutex::new(Some(upgraded_tx)),
                release_rx: Mutex::new(Some(release_rx)),
            })));
        let db = Arc::new(DB::open(opts, dir.path()).unwrap());
        *store.lock() = Arc::downgrade(&db);
        let lifecycle = Arc::clone(&db.worker_lifecycle);

        // Two disjoint L2 files give `compute_split_points` an interior split,
        // forcing `execute_compaction_io` onto the `thread::scope` parallel path
        // once an overlapping L1 file pushes L1 over its 128-byte budget.
        assert_eq!(db.ingest_sorted([(b"a0", b"1"), (b"m0", b"2")]).unwrap(), 2);
        let reopen_opts = DbOptions::default()
            .with_num_levels(3)
            .with_max_subcompactions(2)
            .with_max_bytes_for_level_base(128);
        assert_eq!(db.ingest_sorted([(b"n0", b"3"), (b"z0", b"4")]).unwrap(), 2);
        db.put(b"b0", &[b'x'; 256]).unwrap();
        db.put(b"y0", &[b'y'; 256]).unwrap();
        db.flush().unwrap();

        upgraded_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("parallel subcompaction never entered filter callback");
        drop(db);
        release_tx.send(()).unwrap();

        let (done_tx, done_rx) = mpsc::channel();
        let waiter = thread::spawn(move || {
            lifecycle.wait_until_stopped();
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("workers deadlocked after subcompaction callback dropped final Arc<DB>");
        waiter.join().unwrap();

        let reopened = DB::open(reopen_opts, dir.path()).unwrap();
        assert_eq!(
            reopened.get(b"b0").unwrap().as_deref(),
            Some(&[b'x'; 256][..])
        );
        assert_eq!(
            reopened.get(b"y0").unwrap().as_deref(),
            Some(&[b'y'; 256][..])
        );
        reopened.close().unwrap();
    }

    #[test]
    fn zero_log_number_with_live_sst_rejects_recovery_before_wal_replay_or_tail_repair() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        let sst_path = path.join("000004.sst");
        let sst_meta = {
            let mut b = TableBuilder::new(&sst_path, TableBuildOptions::default()).unwrap();
            let ik = types::InternalKey::new(b"k", 2, ValueType::Value);
            b.add(ik.as_bytes(), b"new").unwrap();
            let res = b.finish().unwrap();
            flush::file_metadata((4, res))
        };

        let wal_path = path.join("000002.wal");
        {
            let mut batch = WriteBatch::new();
            batch.put(b"k", b"old");
            let mut wal = WalWriter::new(&wal_path).unwrap();
            wal.add_record_parts(&[&batch.wal_header(1), &batch.payload])
                .unwrap();
            wal.sync().unwrap();
        }

        let mut vs = VersionSet::create(path, DbOptions::default().num_levels).unwrap();
        let mut edit = VersionEdit::new();
        edit.add_file(0, sst_meta);
        edit.set_last_sequence(2);
        edit.set_next_file_number(5);
        vs.log_and_apply(edit).unwrap();
        let manifest_path = path.join(format!("MANIFEST-{:06}", vs.manifest_number()));
        drop(vs);

        // Append a zero-padded torn fragment header so any premature writable
        // MANIFEST tail truncation would shrink the file.
        let mut manifest_bytes = fs::read(&manifest_path).unwrap();
        manifest_bytes.extend([0u8, 0u8]);
        fs::write(&manifest_path, &manifest_bytes).unwrap();
        let sst_bytes = fs::read(&sst_path).unwrap();
        let wal_bytes = fs::read(&wal_path).unwrap();

        for res in [
            DB::open_read_only(path),
            DB::open(DbOptions::default(), path),
        ] {
            assert_eq!(res.err().unwrap().kind(), ErrorKind::Corruption);
            assert_eq!(fs::read(&manifest_path).unwrap(), manifest_bytes);
            assert_eq!(fs::read(&sst_path).unwrap(), sst_bytes);
            assert_eq!(fs::read(&wal_path).unwrap(), wal_bytes);
        }
    }
}
