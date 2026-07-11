//! Core DB implementation with WAL, MemTable, SST, MANIFEST, and Iterator.

use std::{
    collections::{HashSet, VecDeque},
    fs::{self, OpenOptions},
    io, mem,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar as StdCondvar, Mutex as StdMutex,
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
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::manifest::version_set::VersionSet;
use crate::memtable::MemTable;
use crate::memtable::skiplist::MemTableCursorIter;
use crate::options::{
    CompactionFilter, CompactionFilterDecision, DbOptions, ReadOptions, WriteOptions,
};
use crate::rate_limiter::RateLimiter;
use crate::sst::table_builder::{
    META_BLOCK_SPLIT_THRESHOLD, TableBuildOptions, TableBuildResult, TableBuilder,
};
use crate::sst::table_reader::TableIterator;
use crate::stats::DbStats;
use crate::types::{
    self, MAX_SEQUENCE_NUMBER, MAX_USER_KEY_SIZE, MAX_WRITE_ENTRY_SIZE, SequenceNumber, ValueType,
    WriteBatch, WriteBatchWithIndex,
};
use crate::wal::{WalReader, WalWriter};

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
}

impl DeadKeySweepScheduler {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(DeadKeySweepState::Idle as u8),
        }
    }

    fn queue(&self) {
        let _ = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
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
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
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
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
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
    snapshots: parking_lot::Mutex<Vec<SequenceNumber>>,
}

impl SnapshotList {
    fn new() -> Self {
        Self {
            snapshots: parking_lot::Mutex::new(Vec::new()),
        }
    }

    fn acquire(&self, seq: SequenceNumber) -> SequenceNumber {
        self.snapshots.lock().push(seq);
        seq
    }

    fn release(&self, seq: SequenceNumber) {
        let mut v = self.snapshots.lock();
        if let Some(pos) = v.iter().position(|&s| s == seq) {
            v.swap_remove(pos);
        }
    }

    fn as_sorted_vec(&self) -> Vec<SequenceNumber> {
        let mut v = self.snapshots.lock().clone();
        v.sort_unstable();
        v
    }
}

/// A pending write request for group commit.
struct WriteRequest {
    batch: WriteBatch,
    sync: bool,
    disable_wal: bool,
    result: Option<Result<()>>,
    done: bool,
}

/// Group commit queue state. The `leader_active` flag prevents new arrivals
/// from becoming false "leaders" while the real leader holds `inner` lock.
struct WriteQueueState {
    queue: VecDeque<*mut WriteRequest>,
    leader_active: bool,
}

/// State captured during the freeze phase of a flush.
/// Holds the frozen memtable and metadata needed to complete the flush
/// after releasing the lock.
struct FrozenMemtable {
    old_mem: Arc<MemTable>,
    /// Pre-reserved SST file numbers for the flush outputs. The flush may
    /// split the memtable into multiple SSTs (cut at user-key boundaries)
    /// when projected single-block metadata grows large; numbers are
    /// consumed in order and unused ones are simply never materialized.
    sst_numbers: Vec<u64>,
    old_wal_number: u64,
    new_wal_number: u64,
}

/// Atomically install a fresh SuperVersion into the given `ArcSwap`.
/// Shared between `DB::install_super_version` and compaction threads
/// (which only hold an `Arc<ArcSwap<…>>`, not `&DB`).
fn refresh_super_version(target: &ArcSwap<SuperVersion>, inner: &DBInner) {
    target.store(Arc::new(SuperVersion {
        active_memtable: inner.active_memtable.clone(),
        immutable_memtables: inner.immutable_memtables.clone(),
        version: inner.versions.current(),
    }));
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

/// The core database handle.
pub struct DB {
    path: PathBuf,
    options: DbOptions,
    inner: Arc<Mutex<DBInner>>,
    /// Global sequence number (next to assign).
    sequence: Arc<AtomicU64>,
    /// Last sequence number committed and visible to readers/snapshots.
    committed_sequence: Arc<AtomicU64>,
    /// Write queue and condvar for group commit.
    write_queue: Mutex<WriteQueueState>,
    write_cv: Condvar,
    closed: AtomicBool,
    /// Fast-check flag for background errors (avoids Mutex lock on every read).
    has_bg_error: Arc<AtomicBool>,
    /// Background error (e.g. WAL sync failure). Once set, all subsequent
    /// operations are rejected. Models RocksDB's background error state.
    bg_error: Arc<Mutex<Option<String>>>,
    /// Shared view of `VersionSet::poisoned` (see `poison_flag`). Lets
    /// `check_usable` fail fast on a poisoned MANIFEST without locking
    /// `inner`, and lets sync sites that go through `manifest_sync_handle`
    /// poison the writer when durability cannot be confirmed.
    manifest_poisoned: Arc<AtomicBool>,
    block_cache: Arc<BlockCache>,
    table_cache: Arc<TableCache>,
    rate_limiter: Arc<RateLimiter>,
    stats: Arc<DbStats>,
    /// Shutdown flag for compaction threads.
    compaction_shutdown: Arc<AtomicBool>,
    /// Notification condvar for compaction threads: (has_work, condvar).
    compaction_notify: Arc<(StdMutex<bool>, StdCondvar)>,
    /// Background compaction thread handles.
    compaction_handles: Mutex<Vec<JoinHandle<()>>>,
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
    /// Lock-free read snapshot (SuperVersion).
    /// Readers do a single atomic load. Writers swap atomically after
    /// memtable/version changes — no RwLock contention.
    super_version: Arc<ArcSwap<SuperVersion>>,
    /// Read-triggered compaction hints accumulated from the get path.
    read_compaction_hints: Arc<Mutex<Vec<CompactionHint>>>,
    /// Counter for periodic read-compaction checks.
    read_counter: AtomicU64,
    /// Tracks active snapshots for compaction safety.
    snapshot_list: Arc<SnapshotList>,
    /// Exclusive directory lock (LOCK file). Prevents concurrent DB access.
    /// The File handle holds the flock; released automatically when dropped.
    /// Interior mutability lets explicit `close()` release it before `DB` drops.
    lock_file: Mutex<Option<fs::File>>,
    /// Keys registered for lazy deletion. Checked during compaction
    /// and dropped without writing tombstones.
    dead_keys: Arc<RwLock<HashSet<Vec<u8>>>>,
    /// State machine for automatic dead-key sweeping. New unique registrations
    /// at or above the threshold queue another pass; a request arriving during
    /// a pass is retained for the next pass.
    dead_key_sweep: Arc<DeadKeySweepScheduler>,
}

// SAFETY: the raw `*mut WriteRequest` pointers held in `write_queue` reference
// stack frames of writer threads that remain blocked on `write_cv` (waiting for
// `done`) for the pointers' entire lifetime. Pointers are published and drained
// under the write_queue lock; after draining a group, the single active leader
// holds exclusive ownership of the drained pointers — it may dereference them
// without the lock in `write_batch_group` — until it re-acquires the lock, sets
// `done`, and wakes the owners. No two threads ever access a request concurrently.
// SAFETY: DB's shared mutable state is behind Arc + parking_lot locks or atomics;
// raw request pointers are stack-local to write_queue waiters and never stored in DB.
unsafe impl Send for DB {}
// SAFETY: all cross-thread access is synchronized by locks/atomics; public methods
// do not expose interior raw pointers or unsynchronized mutable references.
unsafe impl Sync for DB {}

struct DBInner {
    active_memtable: Arc<MemTable>,
    immutable_memtables: Vec<Arc<MemTable>>,
    wal_writer: Option<WalWriter>,
    wal_number: u64,
    versions: VersionSet,
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

impl DB {
    /// Open or create a database.
    pub fn open(options: DbOptions, path: impl AsRef<Path>) -> Result<Self> {
        let mut options = options;
        let path = path.as_ref().to_path_buf();

        // A leveled LSM needs at least L0 plus one lower level; `num_levels < 2`
        // would panic during L0 counting / L0→L1 compaction.
        if options.num_levels < 2 {
            return Err(Error::invalid_argument(format!(
                "num_levels must be >= 2, got {}",
                options.num_levels
            )));
        }

        if options.create_if_missing {
            fs::create_dir_all(&path).ctx()?;
        } else if !path.exists() {
            return Err(Error::invalid_argument(format!(
                "DB path does not exist: {}",
                path.display()
            )));
        }

        if options.error_if_exists {
            let current = path.join("CURRENT");
            if current.exists() {
                return Err(Error::invalid_argument(format!(
                    "DB already exists: {}",
                    path.display()
                )));
            }
        }

        // Acquire exclusive directory lock to prevent concurrent DB access
        let lock_file = {
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
            Some(file)
        };

        // Create caches and infra
        let block_cache = Arc::new(match options.block_cache {
            // Shared pool supplied by the caller: attach as a member
            // (block_cache_capacity is ignored — capacity is the pool's).
            Some(ref pool) => pool.attach(),
            // Private single-member pool — the historical behavior.
            None => BlockCache::new(options.block_cache_capacity),
        });
        let rate_limiter = Arc::new(RateLimiter::new(options.rate_limiter_bytes_per_sec));
        let stats = Arc::new(DbStats::new());
        let table_cache = Arc::new(TableCache::new_with_stats(
            &path,
            options.max_open_files,
            Some(block_cache.clone()),
            Some(stats.clone()),
        ));

        // Open or create VersionSet (handles MANIFEST)
        let mut versions =
            VersionSet::open_with_cache(&path, options.num_levels, Some(table_cache.clone()))
                .ctx()?;
        let mut max_sequence = versions.last_sequence();

        // Recover from any WAL files not yet flushed
        let mut active_memtable = Arc::new(MemTable::new());
        let mut wal_numbers: Vec<u64> = Vec::new();
        let mut max_disk_file_number = 0u64;
        for entry in fs::read_dir(&path).ctx()? {
            let entry = entry.ctx()?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            // Track the highest file number present on disk across WAL/SST files.
            if let Some(num) = name
                .strip_suffix(".wal")
                .or_else(|| name.strip_suffix(".sst"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                max_disk_file_number = max_disk_file_number.max(num);
            }
            if let Some(num_str) = name.strip_suffix(".wal")
                && let Ok(num) = num_str.parse::<u64>()
            {
                // Only recover WAL files newer than what's recorded in MANIFEST
                if num >= versions.log_number() {
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
        versions.ensure_file_number_at_least(max_disk_file_number + 1);

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
        for wal_num in &wal_numbers {
            let wal_path = path.join(format!("{:06}.wal", wal_num));
            let mut reader = WalReader::new(&wal_path).ctx()?;
            loop {
                match reader.read_record() {
                    Ok(Some(data)) => {
                        Self::replay_wal_record(&data, &active_memtable, &mut max_sequence)
                            .ctx()?;
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
                        if Some(*wal_num) == recoverable_tail_wal_num
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
        }

        // Create the fresh WAL up front so its number can be recorded in the
        // SAME MANIFEST edit that installs the recovered SST. Otherwise a crash
        // between installing the SST and advancing `log_number` would replay the
        // same WALs again and create a duplicate L0 file with identical keys.
        let wal_number = versions.new_file_number();
        let wal_path = path.join(format!("{:06}.wal", wal_number));
        let wal_writer = WalWriter::new(&wal_path).ctx()?;

        // If we recovered data from WALs, flush it to SST before deleting
        // the old WALs. This ensures the data persists even if we crash again
        // before writing to the new WAL.
        if !wal_numbers.is_empty() && active_memtable.approximate_size() > 0 {
            let make_opts = || TableBuildOptions {
                block_size: options.block_size,
                block_restart_interval: options.block_restart_interval,
                bloom_bits_per_key: options.bloom_bits_per_key,
                internal_keys: true,
                compression: options.compression,
                prefix_len: options.prefix_len,
                block_property_collectors: options
                    .block_property_collectors
                    .iter()
                    .map(|f| f())
                    .collect(),
            };
            let outputs = {
                let mut alloc = || Ok(versions.new_file_number());
                Self::write_memtable_ssts(&active_memtable, &path, &make_opts, &mut alloc).ctx()?
            };

            let mut edit = VersionEdit::new();
            edit.set_log_number(wal_number);
            edit.set_last_sequence(max_sequence);
            edit.set_next_file_number(versions.next_file_number());
            for (sst_number, build_result) in outputs {
                edit.add_file(
                    0,
                    FileMetaData {
                        number: sst_number,
                        file_size: build_result.file_size,
                        smallest_key: build_result.smallest_key.unwrap_or_default(),
                        largest_key: build_result.largest_key.unwrap_or_default(),
                        has_range_deletions: build_result.has_range_deletions,
                    },
                );
            }
            versions.log_and_apply(edit).ctx()?;
            versions.sync_manifest().ctx()?;

            // Reset the memtable — data is now safely in SST
            active_memtable = Arc::new(MemTable::new());
        } else {
            // No recovered data: still record the new log number so the old
            // WALs are skipped on the next open.
            let mut edit = VersionEdit::new();
            edit.set_log_number(wal_number);
            edit.set_next_file_number(versions.next_file_number());
            edit.set_last_sequence(max_sequence);
            versions.log_and_apply(edit).ctx()?;
            versions.sync_manifest().ctx()?;
        }

        // Safe to clean up obsolete files now — the new log_number is durable
        // (so old WALs will never be replayed even if we crash here) and the
        // recovered version set defines the complete live SST set.
        Self::remove_orphan_files(&path, &versions);

        let next_sequence = max_sequence.checked_add(1).ok_or_else(|| {
            Error::invalid_argument("sequence number space exhausted".to_string())
        })?;
        let sequence_start = Arc::new(AtomicU64::new(next_sequence));
        let committed_sequence = Arc::new(AtomicU64::new(max_sequence));

        let manifest_poisoned = versions.poison_flag();

        let inner = Arc::new(Mutex::new(DBInner {
            active_memtable,
            immutable_memtables: Vec::new(),
            wal_writer: Some(wal_writer),
            wal_number,
            versions,
        }));

        // Spawn background compaction threads
        let compaction_shutdown = Arc::new(AtomicBool::new(false));
        let compaction_notify = Arc::new((StdMutex::new(false), StdCondvar::new()));
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
            let l0 = Arc::new(AtomicUsize::new(g.versions.current().l0_file_count()));
            let sv = Arc::new(ArcSwap::from_pointee(SuperVersion {
                active_memtable: g.active_memtable.clone(),
                immutable_memtables: g.immutable_memtables.clone(),
                version: g.versions.current(),
            }));
            (l0, sv)
        };
        let num_compaction_threads = options.max_background_compactions.max(1);
        let mut compaction_handles = Vec::with_capacity(num_compaction_threads);

        let read_compaction_hints = Arc::new(Mutex::new(Vec::<CompactionHint>::new()));
        let snapshot_list = Arc::new(SnapshotList::new());
        for i in 0..num_compaction_threads {
            let bg_inner = Arc::clone(&inner);
            let bg_path = path.clone();
            let bg_options = options.clone();
            let bg_table_cache = table_cache.clone();
            let bg_block_cache = block_cache.clone();
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

            let handle = thread::Builder::new()
                .name(format!("mmdb-compaction-{}", i))
                .spawn(move || {
                    // Helper: set background error and log it.
                    let set_error = |msg: String| {
                        tracing::error!("{}", msg);
                        *bg_error_msg.lock() = Some(msg);
                        bg_has_error.store(true, Ordering::Release);
                    };

                    loop {
                        // Wait for notification
                        {
                            let (lock, cvar) = &*notify;
                            let mut has_work = lock.lock().unwrap();
                            while !*has_work && !shutdown.load(Ordering::Acquire) {
                                has_work = cvar.wait(has_work).unwrap();
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
                                // Drain read-compaction hints (lock released during I/O)
                                {
                                    let hints: Vec<CompactionHint> =
                                        mem::take(&mut *bg_hints.lock());
                                    if !hints.is_empty() {
                                        for hint in &hints {
                                            // Phase 1: pick + pre-allocate (short lock)
                                            let pick = {
                                                let mut inner = bg_inner.lock();
                                                let version = inner.versions.current();
                                                let mut claimed = bg_compacting_files.lock();
                                                LeveledCompaction::pick_compaction_for_hint(
                                                    &version, hint, &claimed,
                                                )
                                                .map(|task| {
                                                    let max_out =
                                                        LeveledCompaction::max_output_files(
                                                            &task,
                                                            &bg_options,
                                                        );
                                                    let file_start = inner
                                                        .versions
                                                        .reserve_file_numbers(max_out);
                                                    let file_limit =
                                                        file_start.saturating_add(max_out);
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
                                                    claimed.extend(claimed_numbers.iter().copied());
                                                    // Capture the snapshot list under the
                                                    // DB lock, consistent with the inputs.
                                                    let active_snaps =
                                                        bg_snapshot_list.as_sorted_vec();
                                                    (
                                                        task,
                                                        file_start,
                                                        file_limit,
                                                        is_bottom,
                                                        active_snaps,
                                                        claimed_numbers,
                                                    )
                                                })
                                            }; // lock released

                                            if let Some((
                                                task,
                                                file_start,
                                                file_limit,
                                                is_bottom,
                                                active_snaps,
                                                claimed_numbers,
                                            )) = pick
                                            {
                                                // Claim released on every exit
                                                // (success or `?`) via Drop.
                                                let _claim = CompactionClaim {
                                                    claims: Arc::clone(&bg_compacting_files),
                                                    numbers: claimed_numbers,
                                                };
                                                // Phase 2: I/O (no lock)
                                                let ctx = CompactionContext {
                                                    db_path: &bg_path,
                                                    options: &bg_options,
                                                    rate_limiter: Some(&bg_rate_limiter),
                                                    stats: Some(&bg_stats),
                                                    active_snapshots: &active_snaps,
                                                };
                                                let output =
                                                    LeveledCompaction::execute_compaction_io(
                                                        &ctx, &task, file_start, file_limit,
                                                        is_bottom,
                                                    )
                                                    .map_err(|e| {
                                                        format!("hint compaction error: {}", e)
                                                    })?;

                                                // Pre-warm the table cache for the new
                                                // output files while unlocked, so the
                                                // install phase's log_and_apply (which
                                                // opens each new file to install its
                                                // reader) hits a warm cache instead of
                                                // parsing footers/indexes under the lock.
                                                bg_table_cache.prewarm(
                                                    output
                                                        .edit
                                                        .new_files
                                                        .iter()
                                                        .map(|(_, meta)| meta.number),
                                                );

                                                // Phase 3: install (short lock; log_and_apply
                                                // still does a directory fsync when new
                                                // files were added, but the per-file SST
                                                // open above is now warm)
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
                                                        )
                                                        .map_err(|e| {
                                                            format!(
                                                                "hint compaction install error: {}",
                                                                e
                                                            )
                                                        })?;
                                                    bg_l0_count.store(
                                                        inner.versions.current().l0_file_count(),
                                                        Ordering::Relaxed,
                                                    );
                                                    refresh_super_version(&bg_sv, &inner);
                                                    cleanup
                                                };
                                                // Phase 4: sync manifest + cleanup (no lock)
                                                {
                                                    let handle = bg_inner
                                                        .lock()
                                                        .versions
                                                        .manifest_sync_handle();
                                                    let mut w = handle.lock();
                                                    if let Some(ref mut writer) = *w
                                                        && let Err(e) = writer.sync()
                                                    {
                                                        // Durability of the applied edit is
                                                        // unconfirmed and a later sync could
                                                        // falsely report it durable — poison
                                                        // the writer (fail-stop, reopen to
                                                        // recover).
                                                        drop(w);
                                                        bg_manifest_poisoned
                                                            .store(true, Ordering::Release);
                                                        return Err(format!(
                                                            "manifest sync error: {}",
                                                            e
                                                        ));
                                                    }
                                                }
                                                LeveledCompaction::run_post_compaction_cleanup(
                                                    &cleanup, &bg_path,
                                                );
                                            }
                                        }
                                    }
                                }

                                // Run pending compactions (lock released during I/O)
                                loop {
                                    // Phase 1: pick + pre-allocate (short lock)
                                    let pick = {
                                        let mut inner = bg_inner.lock();
                                        let version = inner.versions.current();
                                        let mut claimed = bg_compacting_files.lock();
                                        match LeveledCompaction::pick_compaction(
                                            &version,
                                            &bg_options,
                                            &claimed,
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
                                                let max_out = LeveledCompaction::max_output_files(
                                                    &task,
                                                    &bg_options,
                                                );
                                                let file_start =
                                                    inner.versions.reserve_file_numbers(max_out);
                                                let file_limit = file_start.saturating_add(max_out);
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
                                                // Claim all input file numbers so a
                                                // concurrent compaction thread's next
                                                // pick skips them until we install or
                                                // discard (see below).
                                                let claimed_numbers: Vec<u64> = all_inputs
                                                    .iter()
                                                    .map(|f| f.meta.number)
                                                    .collect();
                                                claimed.extend(claimed_numbers.iter().copied());
                                                // Capture the snapshot list under the DB
                                                // lock, consistent with the picked inputs.
                                                let active_snaps = bg_snapshot_list.as_sorted_vec();
                                                Some((
                                                    task,
                                                    file_start,
                                                    file_limit,
                                                    l0_inputs,
                                                    is_bottom,
                                                    active_snaps,
                                                    claimed_numbers,
                                                ))
                                            }
                                            None => None,
                                        }
                                    }; // lock released

                                    let Some((
                                        task,
                                        file_start,
                                        file_limit,
                                        l0_inputs,
                                        is_bottom,
                                        active_snaps,
                                        claimed_numbers,
                                    )) = pick
                                    else {
                                        break;
                                    };
                                    // Claim released on every exit (success or
                                    // `?`) via Drop — dropped before the next
                                    // loop iteration re-picks.
                                    let _claim = CompactionClaim {
                                        claims: Arc::clone(&bg_compacting_files),
                                        numbers: claimed_numbers,
                                    };

                                    // Phase 2: I/O (no lock held)
                                    let ctx = CompactionContext {
                                        db_path: &bg_path,
                                        options: &bg_options,
                                        rate_limiter: Some(&bg_rate_limiter),
                                        stats: Some(&bg_stats),
                                        active_snapshots: &active_snaps,
                                    };
                                    let output = LeveledCompaction::execute_compaction_io(
                                        &ctx, &task, file_start, file_limit, is_bottom,
                                    )
                                    .map_err(|e| format!("compaction error: {}", e))?;

                                    // Pre-warm the table cache for the new output files
                                    // while unlocked (see hint-compaction branch above).
                                    bg_table_cache.prewarm(
                                        output.edit.new_files.iter().map(|(_, meta)| meta.number),
                                    );

                                    // Phase 3: install (short lock; log_and_apply still
                                    // does a directory fsync when new files were added,
                                    // but the per-file SST open above is now warm)
                                    let cleanup = {
                                        let mut inner = bg_inner.lock();
                                        let cleanup = LeveledCompaction::install_compaction(
                                            output,
                                            &mut inner.versions,
                                            Some(&bg_table_cache),
                                            Some(&bg_block_cache),
                                            &bg_path,
                                            Some(&bg_stats),
                                        )
                                        .map_err(|e| format!("compaction install error: {}", e))?;
                                        // Unpin L0 inputs that actually left L0.
                                        // Covers both a normal install (input
                                        // consumed and scheduled for deletion)
                                        // and a trivial move (same physical file
                                        // relabeled to L1, so not in
                                        // files_to_delete). A discarded (stale,
                                        // raced) install leaves the file in L0
                                        // and its first-block pin must stay.
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
                                    {
                                        let handle =
                                            bg_inner.lock().versions.manifest_sync_handle();
                                        let mut w = handle.lock();
                                        if let Some(ref mut writer) = *w
                                            && let Err(e) = writer.sync()
                                        {
                                            // Durability of the applied edit is
                                            // unconfirmed and a later sync could
                                            // falsely report it durable — poison the
                                            // writer (fail-stop, reopen to recover).
                                            drop(w);
                                            bg_manifest_poisoned.store(true, Ordering::Release);
                                            return Err(format!("manifest sync error: {}", e));
                                        }
                                    }
                                    LeveledCompaction::run_post_compaction_cleanup(
                                        &cleanup, &bg_path,
                                    );
                                }

                                // A pending sweep stays queued while snapshots
                                // are active. Once runnable, process deeper
                                // levels first so removing older copies can make
                                // shallower copies eligible in the same pass.
                                loop {
                                    if !bg_dead_key_sweep.is_pending()
                                        || !bg_snapshot_list.as_sorted_vec().is_empty()
                                        || !bg_dead_key_sweep.try_start()
                                    {
                                        break;
                                    }

                                    let mut blocked_by_snapshot = false;
                                    for level in (0..bg_options.num_levels).rev() {
                                        let mut inner = bg_inner.lock();
                                        let active_snaps = bg_snapshot_list.as_sorted_vec();
                                        blocked_by_snapshot |= !active_snaps.is_empty();
                                        let ctx = CompactionContext {
                                            db_path: &bg_path,
                                            options: &bg_options,
                                            rate_limiter: Some(&bg_rate_limiter),
                                            stats: Some(&bg_stats),
                                            active_snapshots: &active_snaps,
                                        };
                                        LeveledCompaction::force_merge_level(
                                            &ctx,
                                            level,
                                            &mut inner.versions,
                                            Some(&bg_table_cache),
                                            Some(&bg_block_cache),
                                        )
                                        .map_err(|e| {
                                            format!("dead-key sweep error at L{}: {}", level, e)
                                        })?;
                                        bg_l0_count.store(
                                            inner.versions.current().l0_file_count(),
                                            Ordering::Relaxed,
                                        );
                                        refresh_super_version(&bg_sv, &inner);
                                    }
                                    bg_dead_key_sweep.finish(blocked_by_snapshot);
                                }
                                Ok(())
                            },
                        ));

                        match result {
                            Ok(Ok(())) => {} // success
                            Ok(Err(msg)) => {
                                set_error(msg);
                                break;
                            }
                            Err(panic_payload) => {
                                let msg = if let Some(s) = panic_payload.downcast_ref::<String>() {
                                    format!("compaction thread panicked: {}", s)
                                } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                                    format!("compaction thread panicked: {}", s)
                                } else {
                                    "compaction thread panicked with unknown payload".to_string()
                                };
                                set_error(msg);
                                break;
                            }
                        }
                    }
                })
                .with_ctx(|| "failed to spawn compaction thread")?;
            compaction_handles.push(handle);
        }

        let db = Self {
            path,
            options,
            inner,
            sequence: sequence_start.clone(),
            committed_sequence,
            write_queue: Mutex::new(WriteQueueState {
                queue: VecDeque::new(),
                leader_active: false,
            }),
            write_cv: Condvar::new(),
            closed: AtomicBool::new(false),
            has_bg_error,
            bg_error,
            manifest_poisoned,
            block_cache,
            table_cache,
            rate_limiter,
            stats,
            compaction_shutdown,
            compaction_notify,
            compaction_handles: Mutex::new(compaction_handles),
            compacting_files,
            l0_file_count,
            super_version,
            read_compaction_hints,
            read_counter: AtomicU64::new(0),
            snapshot_list,
            lock_file: Mutex::new(lock_file),
            dead_keys,
            dead_key_sweep,
        };

        // Kick the background compaction threads once at startup. A DB
        // opened with a pre-existing L0 backlog (e.g. WAL-recovery SSTs
        // accumulated across short-lived processes) would otherwise not
        // compact until the first memtable flush of THIS process signals
        // the background thread — while every write pays the L0-slowdown
        // penalty (and small workloads may never fill a memtable at all,
        // making the stall permanent). The thread re-checks
        // `pick_compaction` and goes back to sleep if there is nothing
        // to do, so an unconditional signal is free.
        db.signal_compaction();

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
        self.check_usable().ctx()?;
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_batch_inner(batch, write_options)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_with_options(&WriteOptions::default(), key)
    }

    pub fn delete_with_options(&self, write_options: &WriteOptions, key: &[u8]) -> Result<()> {
        self.check_usable().ctx()?;
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_batch_inner(batch, write_options)
    }

    /// Delete all keys in the range [begin, end).
    pub fn delete_range(&self, begin: &[u8], end: &[u8]) -> Result<()> {
        self.delete_range_with_options(&WriteOptions::default(), begin, end)
    }

    pub fn delete_range_with_options(
        &self,
        write_options: &WriteOptions,
        begin: &[u8],
        end: &[u8],
    ) -> Result<()> {
        self.check_usable().ctx()?;
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
        self.check_usable().ctx()?;
        if batch.is_empty() {
            return Ok(());
        }
        self.write_batch_inner(batch, write_options)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_with_options(&ReadOptions::default(), key)
    }

    pub fn get_with_options(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_usable().ctx()?;

        let seq = self.resolve_read_sequence(options.snapshot);

        // Lock-free read via SuperVersion.
        let sv = self.get_super_version();
        let (active_mem, imm_mems, version) =
            (&sv.active_memtable, &sv.immutable_memtables, &sv.version);

        // Find the highest-seq range tombstone covering this key (across all memtables).
        // This is needed to compare against any point entry we find.
        let mut max_tomb_seq = self.max_covering_tombstone_seq(key, seq, active_mem, imm_mems);

        // 1. Active MemTable
        if let Some((result, entry_seq)) = active_mem.get_with_seq(key, seq) {
            // If a range tombstone with higher seq exists, it deletes this entry
            if max_tomb_seq > entry_seq {
                return Ok(None);
            }
            if let Some(ref v) = result {
                self.stats.record_read(key.len() as u64 + v.len() as u64);
            }
            return Ok(result);
        }

        // 2. Immutable MemTables (newest first)
        for imm in imm_mems {
            if let Some((result, entry_seq)) = imm.get_with_seq(key, seq) {
                if max_tomb_seq > entry_seq {
                    return Ok(None);
                }
                if let Some(ref v) = result {
                    self.stats.record_read(key.len() as u64 + v.len() as u64);
                }
                return Ok(result);
            }
        }

        // 3. If no point entry in memtables but a tombstone covers the key,
        // the key is deleted (tombstone covers entries in older SSTs too).
        if max_tomb_seq > 0 {
            return Ok(None);
        }

        // 4. L0 SST files (newest first, may overlap).
        // A single memtable flush can be split into several L0 files cut at
        // user-key boundaries; those files are numbered in ascending key order,
        // and a range tombstone is written only to the file holding its start
        // key (it is not fragmented into later split files). A tombstone in a
        // lower-numbered file can therefore cover point keys living in a
        // higher-numbered file from the same flush. Because L0 is scanned
        // newest-first (descending file number), that point key would be found
        // and returned before the tombstone-bearing file is ever visited. So
        // accumulate covering tombstone sequences across ALL L0 files before
        // doing any point lookup, mirroring the L1+ branch and the iterator
        // path. (File number does not track sequence order within one split
        // flush, so the per-file early-out is unsound here.)
        let l0_files = version.level_files(0);
        for tf in l0_files {
            if tf.meta.has_range_deletions {
                let file_tomb_seq = tf.reader.max_covering_tombstone_seq(key, seq).ctx()?;
                max_tomb_seq = max_tomb_seq.max(file_tomb_seq);
            }
        }
        for tf in l0_files {
            if let Some((result, entry_seq)) = tf
                .reader
                .get_internal_with_seq(key, seq, options.fill_cache)
                .ctx()?
            {
                if max_tomb_seq > entry_seq {
                    return Ok(None);
                }
                if let Some(ref v) = result {
                    self.stats.record_read(key.len() as u64 + v.len() as u64);
                }
                return Ok(result);
            }
        }

        // 5. L1+ SST files (sorted by smallest_key, no overlap within level)
        for level in 1..version.num_levels {
            let files = version.level_files(level);
            if files.is_empty() {
                continue;
            }

            // Check range tombstones in all files at this level that have them.
            // We cannot filter by file key bounds because range tombstones can
            // extend past the file's largest_key (table builder bounds only
            // reflect point keys and tombstone start keys, not end keys).
            for tf in files {
                if tf.meta.has_range_deletions {
                    let file_tomb_seq = tf.reader.max_covering_tombstone_seq(key, seq).ctx()?;
                    max_tomb_seq = max_tomb_seq.max(file_tomb_seq);
                }
            }

            // Binary search: find the latest file whose smallest user_key <= key.
            // Files are sorted by smallest_key (internal key), which has user_key prefix.
            let idx = files.partition_point(|tf| {
                let sk = &tf.meta.smallest_key;
                let smallest_uk = if sk.len() >= 8 {
                    &sk[..sk.len() - 8]
                } else {
                    sk.as_slice()
                };
                smallest_uk <= key
            });

            // The candidate file is at idx-1 (last file whose smallest_key <= key)
            if idx == 0 {
                continue;
            }
            let tf = &files[idx - 1];
            let lk = &tf.meta.largest_key;
            let file_largest = if lk.len() >= 8 {
                &lk[..lk.len() - 8]
            } else {
                lk.as_slice()
            };
            if key <= file_largest
                && let Some((result, entry_seq)) = tf
                    .reader
                    .get_internal_with_seq(key, seq, options.fill_cache)
                    .ctx()?
            {
                if max_tomb_seq > entry_seq {
                    return Ok(None);
                }
                // Sample reads at level >= 2 for read-triggered compaction.
                if level >= 2 {
                    self.stats.maybe_sample_read_level(level);
                    self.maybe_check_read_compaction();
                }
                if let Some(ref v) = result {
                    self.stats.record_read(key.len() as u64 + v.len() as u64);
                }
                return Ok(result);
            }
        }

        // If a range tombstone was found in SSTs but no point entry, key is deleted
        if max_tomb_seq > 0 {
            return Ok(None);
        }

        Ok(None)
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
    /// No SST pruning is applied; all files are visited.
    ///
    /// Use `ReadOptions::iterate_lower_bound` / `iterate_upper_bound` to restrict
    /// the key range returned, but note that SST files are still opened for the
    /// full scan. For query-time SST pruning use `iter_with_range()` or
    /// `iter_with_prefix()`.
    pub fn iter_with_options(&self, options: &ReadOptions) -> Result<DBIterator> {
        self.iter_with_range(options, None, None)
    }

    /// Create a forward iterator for an arbitrary key range `[lower_bound, upper_bound)`.
    ///
    /// SST files whose key range does not overlap `[lower_bound, upper_bound)` are
    /// skipped entirely at construction time, avoiding unnecessary block reads.
    /// `None` means unbounded in that direction.
    ///
    /// # When to use
    ///
    /// Use this when the scan range **does not align to a single key prefix** —
    /// for example `[b"m", b"z")` spans many prefixes and cannot be expressed as
    /// a single `iter_with_prefix()` call.
    ///
    /// If your range *does* align to a prefix (e.g. all keys starting with
    /// `b"user:"`), prefer `iter_with_prefix()`: it additionally uses bloom
    /// filters to skip SST files that don't contain the prefix, which is more
    /// precise than range-metadata pruning alone.
    ///
    /// # Pruning comparison
    ///
    /// | Method              | SST range pruning | Bloom filter pruning |
    /// |---------------------|:-----------------:|:--------------------:|
    /// | `iter()`            | ✗                 | ✗                    |
    /// | `iter_with_range()` | ✓                 | ✗                    |
    /// | `iter_with_prefix()`| ✓                 | ✓                    |
    ///
    /// # Bound merging
    ///
    /// Explicit `lower_bound`/`upper_bound` parameters are merged with any bounds
    /// already set in `ReadOptions`, using the tighter of the two.
    pub fn iter_with_range(
        &self,
        options: &ReadOptions,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> Result<DBIterator> {
        self.check_usable().ctx()?;

        let seq = self.resolve_read_sequence(options.snapshot);

        // Lock-free read: use SuperVersion instead of locking inner.
        let sv = self.get_super_version();
        let (active_mem, imm_mems, version) =
            (&sv.active_memtable, &sv.immutable_memtables, &sv.version);

        let est_sources = 1 + imm_mems.len() + version.level_files(0).len() + version.num_levels;
        let mut sources: Vec<IterSource> = Vec::with_capacity(est_sources);
        let mut any_range_deletions = false;

        // Active memtable — cursor-based streaming iterator.
        {
            if active_mem.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(active_mem.clone());
            sources.push(IterSource::from_memtable(cursor).with_level(0));
        }

        // Immutable memtables
        for imm in imm_mems {
            if imm.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(Arc::clone(imm));
            sources.push(IterSource::from_memtable(cursor).with_level(0));
        }

        // SST files — only include files whose key range overlaps [lower_bound, upper_bound].
        // L0 files can overlap each other so we check each individually.
        // L1+ files are sorted and non-overlapping — use a single LevelIterator per level.
        for tf in version.level_files(0) {
            // Range tombstones in this file may cover keys in lower levels
            // even when the file itself is pruned from the merge sources
            // below (file metadata only reflects tombstone *start* keys), so
            // the flag must be set before any pruning decision.
            if tf.meta.has_range_deletions {
                any_range_deletions = true;
            }
            // Check if this file's key range overlaps the bound range.
            if let Some(lo) = lower_bound
                && types::user_key(&tf.meta.largest_key) < lo
            {
                continue;
            }
            if let Some(hi) = upper_bound
                && types::user_key(&tf.meta.smallest_key) > hi
            {
                continue;
            }
            let mut iter =
                TableIterator::new(tf.reader.clone()).with_fill_cache(options.fill_cache);
            if !options.block_property_filters.is_empty() {
                iter = iter.with_block_filters(options.block_property_filters.clone());
            }
            sources.push(IterSource::from_table_iter(iter).with_level(0));
        }
        for level in 1..version.num_levels {
            let files = version.level_files(level);
            if files.is_empty() {
                continue;
            }
            if !any_range_deletions {
                for tf in files {
                    if tf.meta.has_range_deletions {
                        any_range_deletions = true;
                        break;
                    }
                }
            }
            let mut level_iter = LevelIterator::new(files.to_vec())
                .with_fill_cache(options.fill_cache)
                .with_range_hints(
                    lower_bound.map(|s| s.to_vec()),
                    upper_bound.map(|e| e.to_vec()),
                );
            if !options.block_property_filters.is_empty() {
                level_iter = level_iter.with_block_filters(options.block_property_filters.clone());
            }
            sources.push(IterSource::from_level_iter(level_iter).with_level(level));
        }

        let mut db_iter = DBIterator::from_sources(sources, seq);

        // Apply bounds: merge explicit parameters with ReadOptions bounds, using tighter of the two.
        let effective_lower = match (&options.iterate_lower_bound, lower_bound) {
            (Some(opt_lo), Some(param_lo)) => {
                Some(std::cmp::max(opt_lo.as_slice(), param_lo).to_vec())
            }
            (Some(opt_lo), None) => Some(opt_lo.clone()),
            (None, Some(param_lo)) => Some(param_lo.to_vec()),
            (None, None) => None,
        };
        let effective_upper = match (&options.iterate_upper_bound, upper_bound) {
            (Some(opt_hi), Some(param_hi)) => {
                Some(std::cmp::min(opt_hi.as_slice(), param_hi).to_vec())
            }
            (Some(opt_hi), None) => Some(opt_hi.clone()),
            (None, Some(param_hi)) => Some(param_hi.to_vec()),
            (None, None) => None,
        };
        if effective_lower.is_some() || effective_upper.is_some() {
            db_iter.set_bounds(effective_lower, effective_upper);
        }

        // Collect all range tombstones upfront from per-source caches.
        // This enables O(log T) binary search for any key in any direction,
        // replacing the old inline-collection + full-scan preload approach.
        if any_range_deletions {
            // Collect tombstones with level info for cross-level pruning.
            // A tombstone from level L may cover keys at level L or deeper;
            // one strictly deeper than a key's source level never covers it.
            let mut all_tombstones: Vec<(Vec<u8>, Vec<u8>, u64, usize)> = Vec::new();
            // Memtable tombstones are at level 0 (highest priority).
            if active_mem.has_range_deletions() {
                for (b, e, s) in active_mem.get_range_tombstones() {
                    all_tombstones.push((b, e, s, 0));
                }
            }
            for imm in imm_mems {
                if imm.has_range_deletions() {
                    for (b, e, s) in imm.get_range_tombstones() {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
            // L0 files are also at level 0.
            for tf in version.level_files(0) {
                if tf.meta.has_range_deletions {
                    let ts = tf.reader.get_range_tombstones().ctx()?;
                    for (b, e, s) in ts {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
            for level in 1..version.num_levels {
                for tf in version.level_files(level) {
                    if tf.meta.has_range_deletions {
                        // Only skip files whose smallest_key is past the
                        // upper bound.  We cannot use largest_key < lower_bound
                        // to skip because range tombstones can extend past the
                        // file's largest_key.
                        if let Some(hi) = upper_bound
                            && types::user_key(&tf.meta.smallest_key) > hi
                        {
                            continue;
                        }
                        let ts = tf.reader.get_range_tombstones().ctx()?;
                        for (b, e, s) in ts {
                            all_tombstones.push((b, e, s, level));
                        }
                    }
                }
            }
            if !all_tombstones.is_empty() {
                db_iter.set_range_tombstones_with_levels(all_tombstones);
            }
        }
        if let Some(ref sp) = options.skip_point {
            db_iter.set_skip_point(Arc::clone(sp));
        }
        Ok(db_iter)
    }

    /// Create a prefix-bounded iterator with full options support.
    ///
    /// Iterates over all keys that start with `prefix` in order.
    ///
    /// # When to use
    ///
    /// Use this whenever your query is naturally prefix-scoped — for example,
    /// all keys under a tenant (`b"tenant_42:"`), a table (`b"orders:"`), etc.
    /// It is the fastest iterator variant because it applies **two levels of
    /// SST pruning**:
    ///
    /// 1. **Range pruning** — skips SST files whose `[smallest, largest]` key
    ///    range does not overlap the prefix.
    /// 2. **Bloom filter pruning** — among the remaining files, skips those
    ///    whose file-level bloom filter reports that `prefix` is absent.
    ///
    /// For cross-prefix ranges (e.g. `[b"m", b"z")`) use `iter_with_range()`
    /// instead, as there is no single prefix that covers the query.
    ///
    /// # Sub-range within a prefix
    ///
    /// Set `ReadOptions::iterate_lower_bound` / `iterate_upper_bound` to further
    /// restrict iteration to a sub-range inside the prefix.
    pub fn iter_with_prefix(&self, prefix: &[u8], options: &ReadOptions) -> Result<DBIterator> {
        let seq = self.resolve_read_sequence(options.snapshot);
        self.iter_with_prefix_inner(prefix, seq, options)
    }

    /// Create a prefix-bounded iterator at a specific sequence number.
    fn iter_with_prefix_inner(
        &self,
        prefix: &[u8],
        seq: SequenceNumber,
        options: &ReadOptions,
    ) -> Result<DBIterator> {
        self.check_usable().ctx()?;

        // Lock-free read via SuperVersion.
        let sv = self.get_super_version();
        let (active_mem, imm_mems, version) =
            (&sv.active_memtable, &sv.immutable_memtables, &sv.version);

        // Pre-size sources: 1 active + N_imm + N_L0 + N_levels (avoids realloc)
        let est_sources = 1 + imm_mems.len() + version.level_files(0).len() + version.num_levels;
        let mut sources: Vec<IterSource> = Vec::with_capacity(est_sources);
        let mut any_range_deletions = false;

        // Active memtable
        {
            if active_mem.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(active_mem.clone());
            sources.push(IterSource::from_memtable(cursor).with_level(0));
        }

        // Immutable memtables
        for imm in imm_mems {
            if imm.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(Arc::clone(imm));
            sources.push(IterSource::from_memtable(cursor).with_level(0));
        }

        // Compute prefix upper bound once, share across all uses.
        let prefix_owned: Arc<[u8]> = Arc::from(prefix);
        let prefix_upper = {
            let mut upper = prefix.to_vec();
            let mut carry = true;
            for byte in upper.iter_mut().rev() {
                if carry {
                    if *byte == 0xFF {
                        *byte = 0x00;
                    } else {
                        *byte += 1;
                        carry = false;
                    }
                }
            }
            if carry { None } else { Some(upper) }
        };

        // SST files — skip files via range pruning + prefix bloom.
        // L0: per-file filtering (files may overlap).
        for tf in version.level_files(0) {
            // Set the tombstone flag before any pruning: a pruned file's
            // range tombstones may still cover in-range keys in lower levels
            // (metadata and prefix blooms only reflect point keys and
            // tombstone *start* keys).
            if tf.meta.has_range_deletions {
                any_range_deletions = true;
            }
            if types::user_key(&tf.meta.largest_key) < prefix {
                continue;
            }
            if let Some(ref pu) = prefix_upper
                && types::user_key(&tf.meta.smallest_key) >= pu.as_slice()
            {
                continue;
            }
            if !tf.reader.prefix_may_match(prefix) {
                continue;
            }
            let mut iter =
                TableIterator::new(tf.reader.clone()).with_fill_cache(options.fill_cache);
            if !options.block_property_filters.is_empty() {
                iter = iter.with_block_filters(options.block_property_filters.clone());
            }
            sources.push(IterSource::from_table_iter(iter).with_level(0));
        }
        // L1+: one LevelIterator per level with lazy file opening.
        for level in 1..version.num_levels {
            let files = version.level_files(level);
            if files.is_empty() {
                continue;
            }
            if !any_range_deletions {
                for tf in files {
                    if tf.meta.has_range_deletions {
                        any_range_deletions = true;
                        break;
                    }
                }
            }
            let mut level_iter = LevelIterator::new(files.to_vec())
                .with_fill_cache(options.fill_cache)
                .with_prefix(prefix_owned.to_vec())
                .with_range_hints(Some(prefix_owned.to_vec()), prefix_upper.clone());
            if !options.block_property_filters.is_empty() {
                level_iter = level_iter.with_block_filters(options.block_property_filters.clone());
            }
            sources.push(IterSource::from_level_iter(level_iter).with_level(level));
        }

        let mut iter = DBIterator::from_sources_with_prefix(sources, seq, prefix_owned.to_vec());

        // Collect all range tombstones with level info for cross-level pruning.
        if any_range_deletions {
            let mut all_tombstones: Vec<(Vec<u8>, Vec<u8>, u64, usize)> = Vec::new();
            if active_mem.has_range_deletions() {
                for (b, e, s) in active_mem.get_range_tombstones() {
                    all_tombstones.push((b, e, s, 0));
                }
            }
            for imm in imm_mems {
                if imm.has_range_deletions() {
                    for (b, e, s) in imm.get_range_tombstones() {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
            for tf in version.level_files(0) {
                if tf.meta.has_range_deletions {
                    let ts = tf.reader.get_range_tombstones().ctx()?;
                    for (b, e, s) in ts {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
            for level in 1..version.num_levels {
                for tf in version.level_files(level) {
                    if tf.meta.has_range_deletions {
                        // Only skip files whose smallest_key is past the prefix
                        // upper bound.  We cannot use largest_key < prefix
                        // to skip because range tombstones can extend past
                        // the file's largest_key.
                        if let Some(ref pu) = prefix_upper
                            && types::user_key(&tf.meta.smallest_key) >= pu.as_slice()
                        {
                            continue;
                        }
                        let ts = tf.reader.get_range_tombstones().ctx()?;
                        for (b, e, s) in ts {
                            all_tombstones.push((b, e, s, level));
                        }
                    }
                }
            }
            if !all_tombstones.is_empty() {
                iter.set_range_tombstones_with_levels(all_tombstones);
            }
        }

        // Apply ReadOptions bounds if set.
        if options.iterate_lower_bound.is_some() || options.iterate_upper_bound.is_some() {
            iter.set_bounds(
                options.iterate_lower_bound.clone(),
                options.iterate_upper_bound.clone(),
            );
        }

        if let Some(ref sp) = options.skip_point {
            iter.set_skip_point(Arc::clone(sp));
        }

        // Seek to the prefix start (or lower bound if tighter)
        match &options.iterate_lower_bound {
            Some(lb) if lb.as_slice() > prefix => iter.seek(lb),
            _ => iter.seek(prefix),
        }

        Ok(iter)
    }

    /// Create a forward iterator that merges uncommitted batch writes with the
    /// current DB state. The iterator is a snapshot read at the committed
    /// sequence captured here; batch entries are overlaid with sequence
    /// numbers from a reserved region at the top of the sequence space
    /// (`MAX_SEQUENCE_NUMBER - count + 1 ..= MAX_SEQUENCE_NUMBER`), which real
    /// writers can never reach. This gives batch entries precedence over all
    /// committed data without colliding with sequence numbers that concurrent
    /// writers commit after the iterator is created (those stay invisible,
    /// preserving snapshot isolation).
    pub fn iter_with_batch(&self, batch: &WriteBatchWithIndex) -> Result<DBIterator> {
        self.check_usable().ctx()?;

        let seq = self.current_sequence();
        let batch_count = batch.operation_count();
        // Reserve the top of the sequence space for the batch overlay.
        let batch_base_seq = if batch_count == 0 {
            // No overlay entries; the floor is irrelevant but must stay above
            // any committed sequence.
            MAX_SEQUENCE_NUMBER
        } else {
            MAX_SEQUENCE_NUMBER - (batch_count - 1)
        };
        if batch_base_seq <= seq {
            // The committed sequence has reached the reserved region —
            // practically unreachable (2^56 writes), but never alias.
            return Err(Error::invalid_argument(
                "sequence number space exhausted".to_string(),
            ));
        }
        let batch_entries = batch.sorted_entries(batch_base_seq).ctx()?;

        // Lock-free read via SuperVersion.
        let sv = self.get_super_version();
        let (active_mem, imm_mems, version) =
            (&sv.active_memtable, &sv.immutable_memtables, &sv.version);

        let est_sources = 2 + imm_mems.len() + version.level_files(0).len() + version.num_levels;
        let mut sources: Vec<IterSource> = Vec::with_capacity(est_sources);

        // Batch source first (highest priority due to higher sequence numbers).
        sources.push(IterSource::new(batch_entries).with_level(0));

        // Active memtable.
        {
            let cursor = MemTableCursorIter::new(active_mem.clone());
            sources.push(IterSource::from_memtable(cursor).with_level(0));
        }

        // Immutable memtables.
        for imm in imm_mems.iter() {
            let cursor = MemTableCursorIter::new(Arc::clone(imm));
            sources.push(IterSource::from_memtable(cursor).with_level(0));
        }

        // SST files: L0 individually, L1+ via LevelIterator.
        for tf in version.level_files(0) {
            let iter = TableIterator::new(tf.reader.clone());
            sources.push(IterSource::from_table_iter(iter).with_level(0));
        }
        for level in 1..version.num_levels {
            let files = version.level_files(level);
            if files.is_empty() {
                continue;
            }
            let level_iter = LevelIterator::new(files.to_vec());
            sources.push(IterSource::from_level_iter(level_iter).with_level(level));
        }

        let mut db_iter = DBIterator::from_sources(sources, seq);
        if batch_count > 0 {
            db_iter.set_batch_seq_floor(batch_base_seq);
        }

        // Collect range tombstones (same as iter_with_range). Batch iterators
        // lift the tombstone visibility bound to MAX (the batch overlay lives
        // in the reserved top region of the sequence space — see
        // `DBIterator::set_batch_seq_floor`), so DB-sourced tombstones
        // committed after `seq` was captured must be excluded here; other
        // iterator paths instead filter them at query time.
        let mut all_tombstones: Vec<(Vec<u8>, Vec<u8>, u64, usize)> = Vec::new();
        // Batch range tombstones use position-based sequences (preserving write order).
        for &(ref b, ref e, pos) in batch.range_tombstones() {
            all_tombstones.push((b.clone(), e.clone(), batch_base_seq + pos, 0));
        }
        if active_mem.has_range_deletions() {
            for (b, e, s) in active_mem.get_range_tombstones() {
                if s <= seq {
                    all_tombstones.push((b, e, s, 0));
                }
            }
        }
        for imm in imm_mems.iter() {
            if imm.has_range_deletions() {
                for (b, e, s) in imm.get_range_tombstones() {
                    if s <= seq {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
        }
        for tf in version.level_files(0) {
            if tf.meta.has_range_deletions {
                let ts = tf.reader.get_range_tombstones().ctx()?;
                for (b, e, s) in ts {
                    if s <= seq {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
        }
        for level in 1..version.num_levels {
            for tf in version.level_files(level) {
                if tf.meta.has_range_deletions {
                    let ts = tf.reader.get_range_tombstones().ctx()?;
                    for (b, e, s) in ts {
                        if s <= seq {
                            all_tombstones.push((b, e, s, level));
                        }
                    }
                }
            }
        }
        if !all_tombstones.is_empty() {
            db_iter.set_range_tombstones_with_levels(all_tombstones);
        }

        Ok(db_iter)
    }

    pub(crate) fn snapshot_seq(&self) -> SequenceNumber {
        // Register the snapshot under the DB lock so that its sequence number and
        // its registration are atomic with respect to compaction, which captures
        // the snapshot list *after* picking its input files under the same lock.
        // This guarantees a compaction either observes this snapshot, or picks
        // inputs whose maximum sequence is <= this snapshot's sequence (so the
        // snapshot can never need a version the compaction might garbage-collect).
        let _inner = self.inner.lock();
        let seq = self.current_sequence();
        self.snapshot_list.acquire(seq)
    }

    /// Acquire a snapshot with RAII guard. The snapshot is automatically released
    /// when the `Snapshot` is dropped — no manual `release_snapshot()` needed.
    pub fn snapshot(&self) -> Snapshot<'_> {
        let seq = self.snapshot_seq();
        Snapshot { db: self, seq }
    }

    /// Release a previously acquired snapshot so compaction can reclaim its data.
    /// Called by `Snapshot::drop`.
    pub(crate) fn release_snapshot(&self, seq: SequenceNumber) {
        self.snapshot_list.release(seq);
        if self.dead_key_sweep.retry_after_snapshot_release() {
            self.signal_compaction();
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get a database property.
    ///
    /// Supported properties:
    /// - `"num-files-at-level{N}"` — number of SST files at level N
    /// - `"total-sst-size"` — total size of all SST files in bytes
    /// - `"block-cache-usage"` — approximate block cache entry count.
    ///   When `DbOptions::block_cache` attaches this DB to a shared
    ///   [`BlockCachePool`](crate::BlockCachePool), the count is the
    ///   **pool-wide** LRU total across every attached member (plus this
    ///   DB's own pinned entries) — not this DB's share of it.
    /// - `"compaction-pending"` — "1" if compaction is needed, "0" otherwise
    /// - `"stats.bytes_written"` — total user bytes written
    /// - `"stats.bytes_read"` — total user bytes read
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
                let version = inner.versions.current();
                if level < version.num_levels {
                    return Some(version.level_files(level).len().to_string());
                }
            }
            return None;
        }

        match name {
            "total-sst-size" => {
                let version = inner.versions.current();
                let total: u64 = version
                    .files
                    .iter()
                    .flat_map(|level| level.iter())
                    .map(|f| f.meta.file_size)
                    .sum();
                Some(total.to_string())
            }
            "block-cache-usage" => Some(self.block_cache.entry_count().to_string()),
            "compaction-pending" => {
                let version = inner.versions.current();
                // Informational only (not an actual pick+claim), so don't
                // bother excluding in-flight files here.
                let needed =
                    LeveledCompaction::pick_compaction(&version, &self.options, &HashSet::new())
                        .is_some();
                Some(if needed { "1" } else { "0" }.to_string())
            }
            "stats.bytes_written" => {
                Some(self.stats.bytes_written.load(Ordering::Relaxed).to_string())
            }
            "stats.bytes_read" => Some(self.stats.bytes_read.load(Ordering::Relaxed).to_string()),
            "stats.compactions_completed" => Some(
                self.stats
                    .compactions_completed
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.compaction_bytes_written" => Some(
                self.stats
                    .compaction_bytes_written
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.flushes_completed" => Some(
                self.stats
                    .flushes_completed
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.block_cache_hits" => Some(
                self.stats
                    .block_cache_hits
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.block_cache_misses" => Some(
                self.stats
                    .block_cache_misses
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            "stats.cache_hit_rate" => {
                let hits = self.stats.block_cache_hits.load(Ordering::Relaxed) as f64;
                let misses = self.stats.block_cache_misses.load(Ordering::Relaxed) as f64;
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
        self.check_usable().ctx()?;
        let _wg = self.write_queue.lock(); // serialize with writers
        let mut inner = self.inner.lock();
        if inner.active_memtable.is_empty() {
            return Ok(());
        }
        let frozen = self.freeze_memtable_sync(&mut inner).ctx()?;
        drop(inner); // release lock during SST write
        self.flush_and_install_frozen(&frozen).ctx()?;
        let old_wal = frozen.old_wal_number;
        self.post_flush_cleanup(old_wal).ctx()?;
        if self.l0_file_count.load(Ordering::Relaxed) >= self.options.l0_compaction_trigger
            && let Err(e) = self.drain_l0(false)
        {
            // Fatal states: a background error was recorded (e.g. a failed
            // MANIFEST sync inside drain_l0) or the MANIFEST writer is
            // poisoned. Both mean the engine must fail-stop, so propagate.
            let fatal = self.has_bg_error.load(Ordering::Acquire)
                || self.manifest_poisoned.load(Ordering::Acquire);
            if fatal {
                return Err(e).ctx();
            }
            // Non-fatal: the failed step was rejected wholesale before
            // anything was persisted (log_and_apply's error contract) and
            // its output files were already cleaned up. The flush itself —
            // the caller's durability request — has fully succeeded.
            tracing::warn!("post-flush L0 drain failed (will retry): {}", e);
        }
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
        self.check_usable().ctx()?;
        let _wg = self.write_queue.lock();
        self.force_compact_all()
    }

    /// Compact all keys in the given range across all levels.
    /// If `begin` is None, starts from the beginning. If `end` is None, goes to the end.
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.check_usable().ctx()?;

        // First flush memtable to ensure all data is in SSTs
        {
            let _wg = self.write_queue.lock();
            let mut inner = self.inner.lock();
            if !inner.active_memtable.is_empty() {
                let frozen = self.freeze_memtable_sync(&mut inner).ctx()?;
                drop(inner);
                self.flush_and_install_frozen(&frozen).ctx()?;
                let old_wal = frozen.old_wal_number;
                self.post_flush_cleanup(old_wal).ctx()?;
            }
        }

        // Compact files overlapping the specified range
        let _wg = self.write_queue.lock();

        // If no range specified, fall back to full compaction
        if begin.is_none() && end.is_none() {
            return self.force_compact_all();
        }

        // Range-filtered compaction: compact files overlapping [begin, end)
        loop {
            let pick = {
                let mut inner = self.inner.lock();
                let version = inner.versions.current();
                match LeveledCompaction::pick_compaction_for_range(&version, begin, end) {
                    Some(task) => {
                        let l0_inputs: Vec<u64> = if task.level == 0 {
                            task.input_files_level
                                .iter()
                                .map(|f| f.meta.number)
                                .collect()
                        } else {
                            Vec::new()
                        };
                        let max_out = LeveledCompaction::max_output_files(&task, &self.options);
                        let file_start = inner.versions.reserve_file_numbers(max_out);
                        let file_limit = file_start.saturating_add(max_out);
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
                        // Claim the inputs so concurrent background threads
                        // don't redundantly pick and fully re-process the same
                        // files while this unlocked I/O runs. The pick itself
                        // stays claim-blind (`pick_compaction_for_range` takes
                        // no claimed-set): an explicit range compaction must
                        // consider every overlapping file, even one a
                        // background thread is currently rewriting —
                        // `install_compaction`'s stale-input check settles any
                        // race safely, and a discarded install just re-picks.
                        // Filter out numbers already claimed by a background
                        // thread: double-claiming would let this claim's Drop
                        // release the other thread's claim while it still runs.
                        let claimed_inputs: Vec<u64> = {
                            let mut claimed = self.compacting_files.lock();
                            let nums: Vec<u64> = all_inputs
                                .iter()
                                .map(|f| f.meta.number)
                                .filter(|n| !claimed.contains(n))
                                .collect();
                            claimed.extend(nums.iter().copied());
                            nums
                        };
                        Some((
                            task,
                            file_start,
                            file_limit,
                            l0_inputs,
                            is_bottom,
                            active_snaps,
                            claimed_inputs,
                        ))
                    }
                    None => None,
                }
            };

            let Some((task, file_start, file_limit, l0_inputs, is_bottom, active_snaps, claimed)) =
                pick
            else {
                break;
            };
            let _claim = CompactionClaim {
                claims: Arc::clone(&self.compacting_files),
                numbers: claimed,
            };

            let ctx = CompactionContext {
                db_path: &self.path,
                options: &self.options,
                rate_limiter: Some(&self.rate_limiter),
                stats: Some(&self.stats),
                active_snapshots: &active_snaps,
            };
            let output = LeveledCompaction::execute_compaction_io(
                &ctx, &task, file_start, file_limit, is_bottom,
            )
            .ctx()?;

            // Pre-warm the table cache for the new output files while
            // unlocked (see the background compaction loop for rationale).
            self.table_cache
                .prewarm(output.edit.new_files.iter().map(|(_, meta)| meta.number));

            let cleanup = {
                let mut inner = self.inner.lock();
                let cleanup = LeveledCompaction::install_compaction(
                    output,
                    &mut inner.versions,
                    Some(&self.table_cache),
                    Some(&self.block_cache),
                    &self.path,
                    Some(&self.stats),
                )
                .ctx()?;
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
            {
                let handle = self.inner.lock().versions.manifest_sync_handle();
                let mut w = handle.lock();
                if let Some(ref mut writer) = *w
                    && let Err(e) = writer.sync()
                {
                    // The edit is applied in memory but its durability could
                    // not be confirmed — the same fail-stop condition as
                    // drain_l0 and the background threads. Poison the writer
                    // and record the background error so every entry point
                    // sees a fatal engine state, not a retryable hiccup.
                    drop(w);
                    self.manifest_poisoned.store(true, Ordering::Release);
                    self.set_bg_error(format!("compact_range manifest sync failed: {}", e));
                    return Err(e).ctx();
                }
            }
            LeveledCompaction::run_post_compaction_cleanup(&cleanup, &self.path);
        }

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
    /// **Warning:** unlike [`compact`](Self::compact), each level's sweep
    /// rewrite holds the DB's write-serializing lock for its full
    /// duration, stalling writers and new-snapshot creation for however
    /// long the largest populated level takes to rewrite. Only opt in
    /// (set a nonzero threshold) if that stall is acceptable for your
    /// workload; see `DbOptions::lazy_delete_compaction_threshold`.
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
    pub fn lazy_delete(&self, key: &[u8]) {
        let threshold = self.options.lazy_delete_compaction_threshold;
        let mut set = self.dead_keys.write();
        let inserted = set.insert(key.to_vec());
        let len = set.len();
        drop(set);
        if threshold > 0 && len >= threshold && inserted {
            self.request_dead_key_sweep();
        }
    }

    /// Batch version of [`lazy_delete`](Self::lazy_delete).
    ///
    /// Dead keys are **not** persisted — see [`lazy_delete`](Self::lazy_delete)
    /// for durability caveats.
    ///
    /// At or above `lazy_delete_compaction_threshold` (default: `0`, i.e.
    /// disabled — opt-in only), a batch containing any newly registered key
    /// queues a background sweep — see [`lazy_delete`](Self::lazy_delete) for
    /// its exact semantics and the write-stall tradeoff of opting in.
    pub fn lazy_delete_batch(&self, keys: impl IntoIterator<Item = impl AsRef<[u8]>>) {
        let threshold = self.options.lazy_delete_compaction_threshold;
        let mut set = self.dead_keys.write();
        let mut any_new = false;
        for k in keys {
            any_new |= set.insert(k.as_ref().to_vec());
        }
        let len = set.len();
        drop(set);
        if threshold > 0 && len >= threshold && any_new {
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
        self.signal_compaction();
    }

    /// Returns the current number of keys registered for lazy deletion.
    pub fn dead_key_count(&self) -> usize {
        self.dead_keys.read().len()
    }

    /// Clear all dead keys to reclaim memory used by the dead-keys set.
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
    pub fn close(&self) -> Result<()> {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let _wg = self.write_queue.lock();
        let mut inner = self.inner.lock();

        let mut first_error: Option<Error> = None;

        if !inner.active_memtable.is_empty()
            && let Err(e) = self.freeze_and_flush(&mut inner)
        {
            first_error = Some(e);
        }

        if let Some(ref mut wal) = inner.wal_writer
            && let Err(e) = wal.sync()
            && first_error.is_none()
        {
            first_error = Some(e);
        }

        drop(inner);
        drop(_wg);
        self.shutdown_background_and_release_resources();

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    // -- Internal --

    /// Signal background compaction threads (non-blocking).
    fn signal_compaction(&self) {
        let (lock, cvar) = &*self.compaction_notify;
        if let Ok(mut has_work) = lock.lock() {
            *has_work = true;
            cvar.notify_one();
        }
    }

    fn shutdown_background_and_release_resources(&self) {
        {
            let (lock, cvar) = &*self.compaction_notify;
            let _guard = lock.lock().unwrap();
            self.compaction_shutdown.store(true, Ordering::Release);
            cvar.notify_all();
        }
        for handle in self.compaction_handles.lock().drain(..) {
            let _ = handle.join();
        }
        self.block_cache.detach();
        drop(self.lock_file.lock().take());
    }

    /// Find the highest sequence number among range tombstones covering `key`
    /// that are visible at the given read sequence.
    /// Returns 0 if no covering tombstone exists.
    /// Uses the dedicated range tombstone collection for O(T) lookup instead
    /// of O(N) full memtable scan.
    fn max_covering_tombstone_seq(
        &self,
        key: &[u8],
        seq: SequenceNumber,
        active_mem: &MemTable,
        imm_mems: &[Arc<MemTable>],
    ) -> SequenceNumber {
        let mut max_seq: SequenceNumber = 0;

        let s = active_mem.max_covering_tombstone_seq(key, seq);
        if s > max_seq {
            max_seq = s;
        }
        for imm in imm_mems {
            let s = imm.max_covering_tombstone_seq(key, seq);
            if s > max_seq {
                max_seq = s;
            }
        }
        max_seq
    }

    /// Get the current SuperVersion snapshot — single atomic load, truly lock-free.
    fn get_super_version(&self) -> arc_swap::Guard<Arc<SuperVersion>> {
        self.super_version.load()
    }

    /// Refresh the SuperVersion from current inner state.
    /// Called after memtable freeze, flush, or compaction.
    fn install_super_version(&self, inner: &DBInner) {
        refresh_super_version(&self.super_version, inner);
    }

    /// Record a background error, setting the fast-path flag and the detailed message.
    /// All subsequent `check_usable()` calls will return this error.
    fn set_bg_error(&self, msg: String) {
        *self.bg_error.lock() = Some(msg);
        self.has_bg_error.store(true, Ordering::Release);
    }

    /// Periodically check read-level samples and generate compaction hints.
    /// Called every 1024 reads from get_with_options.
    fn maybe_check_read_compaction(&self) {
        let count = self.read_counter.fetch_add(1, Ordering::Relaxed);
        if !count.is_multiple_of(1024) {
            return;
        }
        self.check_read_compaction();
    }

    /// Take read-level samples and convert them into compaction hints.
    fn check_read_compaction(&self) {
        let samples = self.stats.take_read_level_samples();
        let mut hints = self.read_compaction_hints.lock();
        for (level, &count) in samples.iter().enumerate() {
            if count > 0 && level >= 2 {
                hints.push(CompactionHint { level });
            }
        }
        if !hints.is_empty() {
            self.signal_compaction();
        }
    }

    fn check_usable(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::db_closed());
        }
        // Fast path: only lock if the error flag is set.
        if self.has_bg_error.load(Ordering::Acquire)
            && let Some(ref msg) = *self.bg_error.lock()
        {
            return Err(Error::background(msg.clone()));
        }
        // A poisoned MANIFEST writer is a fail-stop condition even when no
        // background error was recorded (e.g. a rotation's directory fsync
        // failed inside `maybe_compact_manifest`, which must not surface an
        // error to its caller). Checked after `bg_error` so the richer
        // message wins when both are set.
        if self.manifest_poisoned.load(Ordering::Acquire) {
            return Err(Error::corruption(
                "MANIFEST writer poisoned by an earlier write failure; \
                 reopen the database to recover"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn current_sequence(&self) -> SequenceNumber {
        self.committed_sequence.load(Ordering::Acquire)
    }

    /// Resolve a caller-supplied read snapshot, clamping it to the committed
    /// sequence. A future sequence (e.g. `u64::MAX`) must not observe entries
    /// from batches that are still being applied to the memtable — the group
    /// commit leader publishes `committed_sequence` only after a batch is
    /// fully inserted, and readers are lock-free, so an unclamped future
    /// snapshot could see a batch half-applied. Snapshots obtained from
    /// `snapshot_seq()` are always <= the committed sequence, so the clamp
    /// never weakens a legitimate snapshot.
    fn resolve_read_sequence(&self, snapshot: Option<SequenceNumber>) -> SequenceNumber {
        let committed = self.current_sequence();
        snapshot.map_or(committed, |s| s.min(committed))
    }

    /// Apply write backpressure based on L0 file count.
    fn maybe_throttle_writes(&self) -> Result<()> {
        // Fast path: check cached L0 count without locking inner.
        let l0_count = self.l0_file_count.load(Ordering::Relaxed);

        if l0_count >= self.options.l0_stop_trigger {
            // Slow path: drain L0 to reduce the file count. Uses drain_l0
            // (not force_compact_all) so an ordinary write only pays for
            // enough compaction to relieve L0 pressure — not a full
            // multi-level database compaction — and does not hold db_mutex
            // for the duration of the merge I/O (drain_l0 follows the
            // standard short-lock pick/install pattern).
            if let Err(e) = self.drain_l0(false) {
                // The stop trigger is the last line of defense against
                // unbounded L0 growth. If compaction is failing, admitting
                // the write would silently disable the stall exactly when it
                // matters most — fail-stop instead, matching the background
                // compaction thread's error policy.
                let msg = format!("inline L0 compaction failed: {}", e);
                self.set_bg_error(msg.clone());
                return Err(Error::background(msg));
            }
            self.l0_file_count.store(
                self.inner.lock().versions.current().l0_file_count(),
                Ordering::Relaxed,
            );
        } else if l0_count >= self.options.l0_slowdown_trigger {
            // Ensure the backlog is actually being worked on. The slowdown
            // state can be entered without any prior signal (e.g. an L0
            // backlog inherited from a previous process at open, before the
            // startup kick has drained it), and signalling is idempotent.
            self.signal_compaction();
            // Progressive delay: more L0 files → longer sleep
            let delay_us = (l0_count - self.options.l0_slowdown_trigger + 1) as u64 * 1000;
            thread::sleep(Duration::from_micros(delay_us));
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
        // Validate entry sizes up front. Oversized keys/values would be
        // accepted into the WAL and memtable but could never be flushed into
        // a readable SST (the reader caps blocks at 64 MiB and the index
        // block stores every data block's boundary keys twice) — rejecting at
        // write time keeps the failure retryable instead of wedging flush.
        for entry in &batch.entries {
            if entry.key.len() > MAX_USER_KEY_SIZE {
                return Err(Error::invalid_argument(format!(
                    "key size {} exceeds maximum {}",
                    entry.key.len(),
                    MAX_USER_KEY_SIZE
                )));
            }
            let val_len = entry.value.as_ref().map_or(0, |v| v.len());
            if entry.key.len().saturating_add(val_len) > MAX_WRITE_ENTRY_SIZE {
                return Err(Error::invalid_argument(format!(
                    "entry size {} (key + value) exceeds maximum {}",
                    entry.key.len() + val_len,
                    MAX_WRITE_ENTRY_SIZE
                )));
            }
        }

        if write_options.no_slowdown {
            let l0_count = self.l0_file_count.load(Ordering::Relaxed);
            if l0_count >= self.options.l0_slowdown_trigger {
                return Err(Error::invalid_argument(
                    "write stalled: no_slowdown is set".to_string(),
                ));
            }
        } else {
            self.maybe_throttle_writes().ctx()?;
        }

        let mut req = WriteRequest {
            batch,
            sync: write_options.sync,
            disable_wal: write_options.disable_wal,
            result: None,
            done: false,
        };
        let req_ptr: *mut WriteRequest = &mut req;

        // Enqueue and check if we should become the leader.
        // The leader_active flag prevents new arrivals from becoming false
        // "leaders" while the real leader holds `inner` — this is critical for
        // group commit to actually batch writes.
        let mut wq = self.write_queue.lock();
        wq.queue.push_back(req_ptr);

        // Wait until either: our request is done (leader processed it), or
        // no leader is active (we should become the leader).
        while !req.done && wq.leader_active {
            self.write_cv.wait(&mut wq);
        }
        if req.done {
            return req.result.take().unwrap_or(Ok(()));
        }

        // Become the leader. The flag ensures only one thread reaches here.
        debug_assert!(!wq.leader_active);
        wq.leader_active = true;

        loop {
            let batch_group: Vec<*mut WriteRequest> = wq.queue.drain(..).collect();
            drop(wq); // release queue lock while doing I/O

            let result = self.write_batch_group(&batch_group);

            // Signal everyone in this batch
            let mut wq_inner = self.write_queue.lock();
            for &rp in &batch_group {
                // SAFETY: all request pointers came from queue entries whose
                // callers are blocked until this leader sets `done`.
                let r = unsafe { &mut *rp };
                if r.result.is_none() {
                    // Error is Clone: every follower receives the leader's
                    // full typed error chain, not a stringified copy.
                    r.result = match &result {
                        Ok(()) => Some(Ok(())),
                        Err(e) => Some(Err(e.clone())),
                    };
                }
                r.done = true;
            }
            self.write_cv.notify_all();

            // Check for stragglers
            if wq_inner.queue.is_empty() {
                wq_inner.leader_active = false;
                self.write_cv.notify_all(); // wake stragglers to compete for leadership
                drop(wq_inner);
                break;
            }
            // More work arrived — loop as leader
            wq = wq_inner;
        }

        req.result.take().unwrap_or(Ok(()))
    }

    /// Process a batch group: assign sequences, write+flush WAL, then publish to MemTable.
    fn write_batch_group(&self, batch_group: &[*mut WriteRequest]) -> Result<()> {
        self.check_usable()?;
        let mut need_sync = false;
        let mut inner = self.inner.lock();

        let total_ops: u64 = batch_group
            .iter()
            .map(|&req_ptr| {
                // SAFETY: request pointers remain valid until the leader marks
                // them done; callers wait on the write queue for that signal.
                unsafe { (&*req_ptr).batch.len() as u64 }
            })
            .sum();
        if total_ops == 0 {
            return Ok(());
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

        let mut assigned = Vec::with_capacity(batch_group.len());
        let mut append_error: Option<String> = None;
        let mut next_seq = first_group_seq;

        for &req_ptr in batch_group {
            // SAFETY: request pointers are owned by waiting writer stack frames
            // and are only accessed by the active leader until completion.
            let r = unsafe { &*req_ptr };
            let first_seq = next_seq;
            next_seq += r.batch.len() as u64;

            if !r.disable_wal {
                let wal_record = Self::encode_wal_record(first_seq, &r.batch);
                if let Some(ref mut wal) = inner.wal_writer
                    && let Err(e) = wal.add_record(&wal_record)
                {
                    append_error = Some(format!("WAL write failed: {}", e));
                    break;
                }
            }
            assigned.push((req_ptr, first_seq));
            need_sync |= r.sync;
        }

        // Single fsync for all batches
        let any_wal = assigned.iter().any(|&(p, _)| {
            // SAFETY: request pointers are still owned by this leader.
            !unsafe { &*p }.disable_wal
        });
        let wal_sync_err = if any_wal {
            match inner.wal_writer {
                Some(ref mut wal) => {
                    let r = if need_sync { wal.sync() } else { wal.flush() };
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
                // SAFETY: request pointers are still owned by this leader.
                let rr = unsafe { &mut *rp };
                rr.result = Some(Err(e.clone()));
            }
            return Err(e);
        }

        let mut applied_last_seq = None;
        for &(req_ptr, first_seq) in &assigned {
            // SAFETY: request pointers are still owned by this leader.
            let r = unsafe { &*req_ptr };
            let mut batch_bytes = 0u64;
            for (i, entry) in r.batch.entries.iter().enumerate() {
                let seq = first_seq + i as u64;
                inner.active_memtable.put(
                    &entry.key,
                    entry.value.as_deref().unwrap_or(&[]),
                    seq,
                    entry.value_type,
                );
                batch_bytes +=
                    entry.key.len() as u64 + entry.value.as_ref().map_or(0, |v| v.len()) as u64;
            }
            if !r.batch.is_empty() {
                applied_last_seq = Some(first_seq + r.batch.len() as u64 - 1);
            }
            self.stats.record_write(batch_bytes);
        }
        if let Some(last_seq) = applied_last_seq {
            self.committed_sequence.store(last_seq, Ordering::Release);
        }

        if let Some(msg) = append_error {
            self.set_bg_error(msg.clone());
            let mut assigned_iter = assigned.iter().map(|&(p, _)| p);
            for &rp in batch_group {
                if assigned_iter.next().is_some_and(|p| p == rp) {
                    // SAFETY: request pointers are still owned by this leader.
                    let rr = unsafe { &mut *rp };
                    rr.result = Some(Ok(()));
                } else {
                    // SAFETY: request pointers are still owned by this leader.
                    let rr = unsafe { &mut *rp };
                    rr.result = Some(Err(Error::io(io::Error::other(msg.clone()))));
                }
            }
            return Ok(());
        }

        // Check memtable size threshold — release lock during SST I/O
        let mut flush_wal: Option<u64> = None;
        if inner.active_memtable.approximate_size() >= self.options.write_buffer_size {
            match self.freeze_memtable_sync(&mut inner) {
                Ok(frozen) => {
                    let old_wal = frozen.old_wal_number;
                    drop(inner); // release lock for slow SST write
                    match self.flush_frozen_memtable(&frozen) {
                        Ok(build_results) => {
                            let output_numbers: Vec<u64> =
                                build_results.iter().map(|(n, _)| *n).collect();
                            let mut inner = self.inner.lock(); // reacquire
                            match self.install_flush(&mut inner, &frozen, build_results) {
                                Ok(()) => flush_wal = Some(old_wal),
                                Err(e) => {
                                    // Clean up orphan SST files: log_and_apply did not
                                    // persist the edit, so these files must be deleted.
                                    for num in &output_numbers {
                                        let path = self.path.join(format!("{:06}.sst", num));
                                        let _ = fs::remove_file(&path);
                                    }
                                    // The frozen memtable is still live in
                                    // `immutable_memtables` and its WAL is intact, but a
                                    // later successful flush would advance `log_number`
                                    // past this WAL (stale reads + lost recovery). Fail-stop.
                                    self.set_bg_error(format!("flush install failed: {}", e));
                                }
                            }
                            drop(inner);
                        }
                        Err(e) => {
                            // Same hazard as an install failure: the unflushed memtable
                            // must not be skipped by a later flush. Fail-stop so its WAL
                            // stays recoverable and no stale state is published.
                            self.set_bg_error(format!("auto-flush SST write failed: {}", e));
                        }
                    }
                }
                Err(e) => {
                    self.set_bg_error(format!("auto-flush freeze failed: {}", e));
                    drop(inner);
                }
            }
        } else {
            drop(inner);
        }

        if let Some(old_wal) = flush_wal {
            if let Err(e) = self.post_flush_cleanup(old_wal) {
                // post_flush_cleanup already fail-stops (set_bg_error) on
                // manifest-sync failure; just log here since this path
                // can't propagate a Result to a caller.
                tracing::warn!("post-flush cleanup failed for WAL {}: {}", old_wal, e);
            }
            self.signal_compaction();
        }

        Ok(())
    }

    /// Freeze+flush in one call (holds lock throughout).
    /// Used by `close()` where the lock is already held
    /// and releasing it would complicate the caller.
    fn freeze_and_flush(&self, inner: &mut DBInner) -> Result<()> {
        let frozen = self.freeze_memtable_sync(inner).ctx()?;
        // After a successful freeze the immutable memtable + its WAL are live; any
        // failure before the install + manifest sync complete must fail-stop, or a
        // later flush could advance log_number past this WAL (stale reads + lost
        // recovery on restart).
        let build_results = match self.flush_frozen_memtable(&frozen) {
            Ok(r) => r,
            Err(e) => {
                self.set_bg_error(format!("flush SST write failed: {}", e));
                return Err(e);
            }
        };
        let output_numbers: Vec<u64> = build_results.iter().map(|(n, _)| *n).collect();
        if let Err(e) = self.install_flush(inner, &frozen, build_results) {
            // Clean up orphan SST files produced by the failed flush install.
            for num in &output_numbers {
                let path = self.path.join(format!("{:06}.sst", num));
                let _ = fs::remove_file(&path);
            }
            self.set_bg_error(format!("flush install failed: {}", e));
            return Err(e);
        }
        // Sync inline (close path, not performance-critical)
        if let Err(e) = inner.versions.sync_manifest() {
            self.set_bg_error(format!("flush manifest sync failed: {}", e));
            return Err(e);
        }
        let old_wal_path = self.path.join(format!("{:06}.wal", frozen.old_wal_number));
        if let Err(e) = fs::remove_file(&old_wal_path) {
            tracing::warn!("failed to remove old WAL {}: {}", old_wal_path.display(), e);
        }
        Ok(())
    }

    /// Flush a frozen memtable to SST and install it, fail-stopping on any failure.
    /// Used by the explicit `flush()` / `compact_range()` paths, which release the
    /// DB lock during the SST write. A failure after the freeze leaves the frozen
    /// memtable live in `immutable_memtables`; without fail-stop a later successful
    /// flush would advance `log_number` past this memtable's WAL.
    fn flush_and_install_frozen(&self, frozen: &FrozenMemtable) -> Result<()> {
        let build_results = match self.flush_frozen_memtable(frozen) {
            Ok(r) => r,
            Err(e) => {
                self.set_bg_error(format!("flush SST write failed: {}", e));
                return Err(e);
            }
        };
        let output_numbers: Vec<u64> = build_results.iter().map(|(n, _)| *n).collect();
        let mut inner = self.inner.lock();
        if let Err(e) = self.install_flush(&mut inner, frozen, build_results) {
            drop(inner);
            // Clean up orphan SST files: log_and_apply did not persist the edit.
            for num in &output_numbers {
                let path = self.path.join(format!("{:06}.sst", num));
                let _ = fs::remove_file(&path);
            }
            self.set_bg_error(format!("flush install failed: {}", e));
            return Err(e);
        }
        Ok(())
    }

    /// Phase 1 (under lock, fast): swap memtable, create WAL, reserve SST numbers.
    /// Returns the FrozenMemtable for synchronous processing without queueing for background threads.
    fn freeze_memtable_sync(&self, inner: &mut DBInner) -> Result<FrozenMemtable> {
        let new_wal_number = inner.versions.new_file_number();
        let new_wal_path = self.path.join(format!("{:06}.wal", new_wal_number));
        let new_wal = WalWriter::new(&new_wal_path).ctx()?;
        let old_mem = mem::replace(&mut inner.active_memtable, Arc::new(MemTable::new()));
        let old_wal_number = inner.wal_number;
        inner.wal_writer = Some(new_wal);
        inner.wal_number = new_wal_number;
        inner.immutable_memtables.push(old_mem.clone());

        // Reserve enough file numbers for the flush to split its output when
        // projected single-block metadata grows large (key-heavy data).
        // Generous over-reservation is harmless: file numbers come
        // from a monotonic u64 counter and unused ones are never reused.
        let reserve =
            2 + (4 * old_mem.approximate_size() as u64) / META_BLOCK_SPLIT_THRESHOLD as u64;
        let base = inner.versions.reserve_file_numbers(reserve);
        let sst_numbers: Vec<u64> = (base..base + reserve).collect();
        self.install_super_version(inner);

        Ok(FrozenMemtable {
            old_mem,
            sst_numbers,
            old_wal_number,
            new_wal_number,
        })
    }

    /// Phase 2 (no lock needed, slow I/O): write SSTs from frozen memtable.
    fn flush_frozen_memtable(
        &self,
        frozen: &FrozenMemtable,
    ) -> Result<Vec<(u64, TableBuildResult)>> {
        let mut numbers = frozen.sst_numbers.iter().copied();
        let results = Self::write_memtable_ssts(
            &frozen.old_mem,
            &self.path,
            &|| self.flush_build_opts(),
            &mut || {
                numbers.next().ok_or_else(|| {
                    Error::invalid_argument(
                        "reserved flush output file numbers exhausted".to_string(),
                    )
                })
            },
        )?;
        // Pre-warm the table cache for the new SSTs while unlocked, so
        // install_flush's log_and_apply (which opens each new file to
        // install its reader) hits a warm cache instead of parsing
        // footers/indexes while db_mutex is held.
        self.table_cache
            .prewarm(results.iter().map(|(number, _)| *number));
        Ok(results)
    }

    /// Phase 3 (under lock, fast): install flush result into version.
    fn install_flush(
        &self,
        inner: &mut DBInner,
        frozen: &FrozenMemtable,
        build_results: Vec<(u64, TableBuildResult)>,
    ) -> Result<()> {
        let mut edit = VersionEdit::new();
        edit.set_log_number(frozen.new_wal_number);
        edit.set_next_file_number(inner.versions.next_file_number());
        edit.set_last_sequence(self.current_sequence());
        let output_numbers: Vec<u64> = build_results.iter().map(|(n, _)| *n).collect();
        for (number, build_result) in build_results {
            edit.add_file(
                0, // L0
                FileMetaData {
                    number,
                    file_size: build_result.file_size,
                    smallest_key: build_result.smallest_key.unwrap_or_default(),
                    largest_key: build_result.largest_key.unwrap_or_default(),
                    has_range_deletions: build_result.has_range_deletions,
                },
            );
        }
        inner.versions.log_and_apply(edit).ctx()?;
        self.stats.record_flush();
        inner
            .immutable_memtables
            .retain(|m| !Arc::ptr_eq(m, &frozen.old_mem));

        if self.options.pin_l0_filter_and_index_blocks_in_cache {
            let version = inner.versions.current();
            for tf in version.level_files(0) {
                if output_numbers.contains(&tf.meta.number) {
                    tf.reader.pin_metadata_in_cache();
                }
            }
        }

        self.l0_file_count
            .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
        self.install_super_version(inner);

        // WAL deletion is deferred: callers must sync the manifest first,
        // then delete the old WAL file outside the main lock.
        Ok(())
    }

    /// Sync the manifest and delete the old WAL after install_flush.
    /// Must be called outside the main lock.
    ///
    /// By the time this runs, `install_flush` has already applied the
    /// VersionEdit and removed the memtable from `immutable_memtables`, so
    /// the only remaining risk is manifest durability: a later successful
    /// flush would advance `log_number` past this WAL (stale reads + lost
    /// recovery), so a sync failure here fail-stops via `set_bg_error` for
    /// every caller (explicit `flush()`/`compact_range()`, and the hot
    /// auto-flush path, which cannot otherwise propagate this failure to
    /// any caller since the enclosing write still needs to return `Ok`).
    fn post_flush_cleanup(&self, old_wal_number: u64) -> Result<()> {
        {
            let handle = self.inner.lock().versions.manifest_sync_handle();
            let mut w = handle.lock();
            if let Some(ref mut writer) = *w
                && let Err(e) = writer.sync()
            {
                // Poison first: a later "successful" sync could falsely
                // report durability of the records this sync failed to
                // persist (fsyncgate).
                drop(w);
                self.manifest_poisoned.store(true, Ordering::Release);
                self.set_bg_error(format!("post-flush manifest sync failed: {}", e));
                return Err(e).ctx();
            }
        }
        let old_wal_path = self.path.join(format!("{:06}.wal", old_wal_number));
        if let Err(e) = fs::remove_file(&old_wal_path) {
            tracing::warn!("failed to remove old WAL {}: {}", old_wal_path.display(), e);
        }
        Ok(())
    }

    /// Drain L0 down below its compaction trigger. Follows the same
    /// short-lock pick → unlocked I/O → short-lock install → unlocked sync
    /// pattern used by the background compaction thread pool
    /// (`.claude/docs/patterns/concurrency.md` INV-CC4), so this does not
    /// stall other `db_mutex` consumers (writers, other compactions, admin
    /// calls) for the duration of the merge I/O — unlike the previous
    /// `do_compaction`, which held the lock for its entire run. Used by the
    /// inline write-throttle path and by `flush()`'s post-flush trigger,
    /// neither of which should pay for a full-database compaction, and by
    /// `force_compact_all`.
    ///
    /// When a pick is blocked because another in-flight compaction has
    /// claimed the current L0 files, `wait_for_inflight` selects the policy:
    /// `false` returns immediately (opportunistic callers — the in-flight
    /// compaction is already doing the work), while `true` waits for the
    /// claim to settle and re-picks so L0 is genuinely drained on return
    /// (required by `force_compact_all`, whose level passes would otherwise
    /// run before L0 data reached them).
    fn drain_l0(&self, wait_for_inflight: bool) -> Result<()> {
        let force_opts = DbOptions {
            l0_compaction_trigger: 1,
            ..self.options.clone()
        };
        loop {
            // Phase 1: pick + pre-allocate (short lock)
            let (pick, l0_blocked) = {
                let mut inner = self.inner.lock();
                let version = inner.versions.current();
                let mut claimed = self.compacting_files.lock();
                match LeveledCompaction::pick_compaction(&version, &force_opts, &claimed) {
                    Some(task) => {
                        let l0_inputs: Vec<u64> = if task.level == 0 {
                            task.input_files_level
                                .iter()
                                .map(|f| f.meta.number)
                                .collect()
                        } else {
                            Vec::new()
                        };
                        let max_out = LeveledCompaction::max_output_files(&task, &force_opts);
                        let file_start = inner.versions.reserve_file_numbers(max_out);
                        let file_limit = file_start.saturating_add(max_out);
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
                                file_start,
                                file_limit,
                                l0_inputs,
                                is_bottom,
                                active_snaps,
                                claimed_numbers,
                            )),
                            false,
                        )
                    }
                    // With `l0_compaction_trigger: 1`, a `None` pick while L0
                    // is non-empty means every candidate was excluded by an
                    // in-flight claim — not that L0 is drained.
                    None => (None, version.l0_file_count() > 0),
                }
            }; // lock released
            let Some((
                task,
                file_start,
                file_limit,
                l0_inputs,
                is_bottom,
                active_snaps,
                claimed_numbers,
            )) = pick
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
                db_path: &self.path,
                options: &force_opts,
                rate_limiter: Some(&self.rate_limiter),
                stats: Some(&self.stats),
                active_snapshots: &active_snaps,
            };
            let output = LeveledCompaction::execute_compaction_io(
                &ctx, &task, file_start, file_limit, is_bottom,
            )
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
                    &self.path,
                    Some(&self.stats),
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
            {
                let handle = self.inner.lock().versions.manifest_sync_handle();
                let mut w = handle.lock();
                if let Some(ref mut writer) = *w
                    && let Err(e) = writer.sync()
                {
                    // The edit is applied in memory but its durability could
                    // not be confirmed — the same condition the background
                    // thread treats as fail-stop. Poison the writer and
                    // record it so `flush()` and `check_usable` see a fatal
                    // engine state, not a retryable hiccup.
                    drop(w);
                    self.manifest_poisoned.store(true, Ordering::Release);
                    self.set_bg_error(format!("drain_l0 manifest sync failed: {}", e));
                    return Err(e).ctx();
                }
            }
            LeveledCompaction::run_post_compaction_cleanup(&cleanup, &self.path);
        }
        Ok(())
    }

    /// Drain L0, then force-merge every level down to as few files as
    /// possible. This is the full, deliberate compaction used by the
    /// explicit, user-invoked `compact()` / `compact_range(None, None)`
    /// entry points — NOT by the inline write-throttle path or `flush()`,
    /// which only need `drain_l0` to relieve L0 backpressure. The lock is
    /// released between `drain_l0` and each level (and between levels), so
    /// this does not hold `db_mutex` for the whole operation in one shot,
    /// though each individual level's merge I/O is still performed under
    /// lock (`force_merge_level` requires `&mut VersionSet` throughout) —
    /// acceptable here since this path is administrative, not the write hot
    /// path that `drain_l0` was split out to protect.
    fn force_compact_all(&self) -> Result<()> {
        // Settle in-flight background compactions first. Their merge streams
        // were filtered when they were picked, so decisions may predate
        // dead-key registrations (or other filter-state changes) made before
        // this call. Waiting lets them install — their outputs are then
        // rewritten by the passes below — or discard, instead of installing
        // stale-filtered data after this full pass has returned.
        self.compacting_files.wait_until_empty();
        self.drain_l0(true)?;
        let force_opts = DbOptions {
            l0_compaction_trigger: 1,
            ..self.options.clone()
        };
        for level in 1..self.options.num_levels {
            let mut inner = self.inner.lock();
            let active_snaps = self.snapshot_list.as_sorted_vec();
            let ctx = CompactionContext {
                db_path: &self.path,
                options: &force_opts,
                rate_limiter: Some(&self.rate_limiter),
                stats: Some(&self.stats),
                active_snapshots: &active_snaps,
            };
            LeveledCompaction::force_merge_level(
                &ctx,
                level,
                &mut inner.versions,
                Some(&self.table_cache),
                Some(&self.block_cache),
            )
            .ctx()?;
            self.l0_file_count
                .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
            self.install_super_version(&inner);
        }
        Ok(())
    }

    fn encode_wal_record(sequence: u64, batch: &WriteBatch) -> Vec<u8> {
        // Pre-size: 8 (seq) + 4 (count) + per-entry (1 type + 4 key_len + key + 4 val_len + val)
        let estimated = 12
            + batch
                .entries
                .iter()
                .map(|e| 1 + 4 + e.key.len() + 4 + e.value.as_ref().map_or(0, |v| v.len()))
                .sum::<usize>();
        let mut buf = Vec::with_capacity(estimated);
        buf.extend_from_slice(&sequence.to_le_bytes());
        buf.extend_from_slice(&(batch.len() as u32).to_le_bytes());
        for entry in &batch.entries {
            buf.push(entry.value_type as u8);
            buf.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&entry.key);
            if entry.value_type == ValueType::Value || entry.value_type == ValueType::RangeDeletion
            {
                let val = entry.value.as_deref().unwrap_or(&[]);
                buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
                buf.extend_from_slice(val);
            }
        }
        buf
    }

    /// Remove files in the DB directory that recovery has proven dead:
    /// - `.wal` files numbered below the recovered `log_number` — already
    ///   replayed (or superseded) and never read again,
    /// - `.sst` files absent from the recovered version — crash orphans from
    ///   an interrupted flush/compaction, or compaction inputs whose deletion
    ///   edit was durable but whose unlink never ran,
    /// - `MANIFEST-*` files other than the current one, plus `CURRENT.tmp` /
    ///   `CURRENT.tmp.<N>` — leftovers from an interrupted MANIFEST rotation
    ///   (the unsuffixed form is only produced by pre-4.1.1 versions).
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
                // Keep the unsuffixed literal: dirs written by pre-4.1.1
                // versions may still hold a plain `CURRENT.tmp`.
                name == "CURRENT.tmp" || name.starts_with("CURRENT.tmp.")
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

    fn replay_wal_record(data: &[u8], mem: &MemTable, max_sequence: &mut u64) -> Result<()> {
        if data.len() < 12 {
            return Err(Error::corruption(format!(
                "WAL record too short: {} bytes",
                data.len()
            )));
        }
        let seq = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let count = u32::from_le_bytes(data[8..12].try_into().unwrap());
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
            *max_sequence = (*max_sequence).max(entry_seq);

            let vt = data[offset];
            offset += 1;
            if offset + 4 > data.len() {
                return Err(Error::corruption(format!(
                    "WAL record truncated reading key length at entry {}",
                    i
                )));
            }
            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "WAL record truncated reading key at entry {}",
                    i
                )));
            }
            let key = &data[offset..offset + key_len];
            offset += key_len;

            match ValueType::from_u8(vt) {
                Some(ValueType::Value) => {
                    if offset + 4 > data.len() {
                        return Err(Error::corruption(format!(
                            "WAL record truncated reading value length at entry {}",
                            i
                        )));
                    }
                    let val_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + val_len > data.len() {
                        return Err(Error::corruption(format!(
                            "WAL record truncated reading value at entry {}",
                            i
                        )));
                    }
                    let value = &data[offset..offset + val_len];
                    offset += val_len;
                    mem.put(key, value, entry_seq, ValueType::Value);
                }
                Some(ValueType::Deletion) => {
                    mem.put(key, &[], entry_seq, ValueType::Deletion);
                }
                Some(ValueType::RangeDeletion) => {
                    // RangeDeletion: value is the end key
                    if offset + 4 > data.len() {
                        return Err(Error::corruption(format!(
                            "WAL record truncated reading range-del end key length at entry {}",
                            i
                        )));
                    }
                    let val_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + val_len > data.len() {
                        return Err(Error::corruption(format!(
                            "WAL record truncated reading range-del end key at entry {}",
                            i
                        )));
                    }
                    let value = &data[offset..offset + val_len];
                    offset += val_len;
                    mem.put(key, value, entry_seq, ValueType::RangeDeletion);
                }
                None => {
                    return Err(Error::corruption(format!(
                        "WAL record contains unknown value type {} at entry {}",
                        vt, i
                    )));
                }
            }
        }
        Ok(())
    }

    /// Build options for flush outputs (always L0).
    fn flush_build_opts(&self) -> TableBuildOptions {
        let compression = if !self.options.compression_per_level.is_empty() {
            self.options.compression_per_level[0]
        } else {
            self.options.compression
        };
        TableBuildOptions {
            block_size: self.options.block_size,
            block_restart_interval: self.options.block_restart_interval,
            bloom_bits_per_key: self.options.bloom_bits_per_key,
            internal_keys: true,
            compression,
            prefix_len: self.options.prefix_len,
            block_property_collectors: self
                .options
                .block_property_collectors
                .iter()
                .map(|f| f())
                .collect(),
        }
    }

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
        next_number: &mut dyn FnMut() -> Result<u64>,
    ) -> Result<Vec<(u64, TableBuildResult)>> {
        let cleanup = |results: &[(u64, TableBuildResult)], current: Option<u64>| {
            for (num, _) in results {
                let _ = fs::remove_file(db_path.join(format!("{:06}.sst", num)));
            }
            if let Some(num) = current {
                let _ = fs::remove_file(db_path.join(format!("{:06}.sst", num)));
            }
        };

        let mut results: Vec<(u64, TableBuildResult)> = Vec::new();
        let mut builder: Option<(u64, TableBuilder)> = None;
        let mut pending_cut = false;
        let mut last_uk: Vec<u8> = Vec::new();

        for (key, value) in mem.iter() {
            let uk_changed = types::user_key(&key) != last_uk.as_slice();
            if uk_changed {
                last_uk.clear();
                last_uk.extend_from_slice(types::user_key(&key));
            }
            if pending_cut
                && uk_changed
                && let Some((num, b)) = builder.take()
            {
                match b.finish().ctx() {
                    Ok(result) => results.push((num, result)),
                    Err(e) => {
                        cleanup(&results, Some(num));
                        return Err(e);
                    }
                }
                pending_cut = false;
            }
            if builder.is_none() {
                let num = match next_number() {
                    Ok(n) => n,
                    Err(e) => {
                        cleanup(&results, None);
                        return Err(e).ctx();
                    }
                };
                let sst_path = db_path.join(format!("{:06}.sst", num));
                match TableBuilder::new(&sst_path, make_opts()).ctx() {
                    Ok(b) => builder = Some((num, b)),
                    Err(e) => {
                        cleanup(&results, None);
                        return Err(e);
                    }
                }
            }
            let (num, b) = builder.as_mut().unwrap();
            if let Err(e) = b.add(&key, &value).ctx() {
                let num = *num;
                cleanup(&results, Some(num));
                return Err(e);
            }
            if b.projected_meta_size() >= META_BLOCK_SPLIT_THRESHOLD {
                pending_cut = true;
            }
        }
        if let Some((num, b)) = builder.take() {
            match b.finish().ctx() {
                Ok(result) => results.push((num, result)),
                Err(e) => {
                    cleanup(&results, Some(num));
                    return Err(e);
                }
            }
        }
        Ok(results)
    }
}

/// Test utilities gated behind the `test-utils` feature.
#[cfg(any(test, feature = "test-utils"))]
impl DB {
    /// Simulate a crash: shut down background threads without flushing memtable.
    /// Useful for testing WAL recovery without zombie compaction threads.
    pub fn simulate_crash(self) {
        self.closed.store(true, Ordering::Release);
        self.shutdown_background_and_release_resources();
        mem::forget(self);
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::Acquire) {
            self.closed.store(true, Ordering::Release);
            if let Some(ref mut wal) = self.inner.lock().wal_writer {
                // Note: WAL sync errors in Drop cannot be propagated to the caller.
                // Users should call db.close() explicitly to detect and handle sync errors.
                let _ = wal.sync();
            }
        }
        self.shutdown_background_and_release_resources();
    }
}

/// RAII snapshot guard. Automatically releases the snapshot when dropped.
///
/// Use `DB::snapshot()` to create one. The snapshot's sequence number can be
/// passed to `ReadOptions::snapshot` for consistent reads.
pub struct Snapshot<'a> {
    db: &'a DB,
    seq: SequenceNumber,
}

impl Snapshot<'_> {
    /// The sequence number for this snapshot.
    pub fn sequence(&self) -> SequenceNumber {
        self.seq
    }

    /// Return `ReadOptions` pre-configured to read at this snapshot.
    pub fn read_options(&self) -> ReadOptions {
        ReadOptions {
            snapshot: Some(self.seq),
            ..ReadOptions::default()
        }
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        self.db.release_snapshot(self.seq);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let snap = db.snapshot_seq();
        db.put(b"key", b"v2").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"v2".to_vec()));
        let opts = ReadOptions {
            snapshot: Some(snap),
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
            db.closed.store(true, Ordering::Release);
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
            db.closed.store(true, Ordering::Release);
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
        let snap = db.snapshot_seq();
        db.put(b"a", b"v2").unwrap();
        db.put(b"b", b"new").unwrap();

        // Iterator at snapshot should see old value and no "b"
        let opts = ReadOptions {
            snapshot: Some(snap),
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
}
