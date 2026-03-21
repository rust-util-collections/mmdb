//! Core DB implementation with WAL, MemTable, SST, MANIFEST, and Iterator.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::ArcSwap;
use parking_lot::{Condvar, Mutex};

use crate::cache::block_cache::BlockCache;
use crate::cache::table_cache::TableCache;
use crate::compaction::LeveledCompaction;
use crate::compaction::leveled::CompactionHint;
use crate::error::{Error, Result};
use crate::iterator::LevelIterator;
use crate::iterator::db_iter::DBIterator;
use crate::iterator::merge::IterSource;
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::manifest::version_set::VersionSet;
use crate::memtable::MemTable;
use crate::memtable::skiplist::MemTableCursorIter;
use crate::options::{DbOptions, ReadOptions, WriteOptions};
use crate::rate_limiter::RateLimiter;
use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
use crate::sst::table_reader::TableIterator;
use crate::stats::DbStats;
use crate::types::{self, SequenceNumber, ValueType, WriteBatch, WriteBatchWithIndex};
use crate::wal::{WalReader, WalWriter};
use ruc::*;

/// Global pool of reusable DBIterator objects.
/// Avoids heap allocation overhead when creating iterators in tight loops.
/// Uses a lock-free-contention global pool instead of thread-local storage
/// so that iterators can be recycled across threads (e.g., thread-per-request
/// servers, tokio spawn_blocking).
static POOL_CAPACITY: LazyLock<usize> = LazyLock::new(|| {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    (cpus * 4).clamp(32, 512)
});

static GLOBAL_ITER_POOL: LazyLock<Mutex<Vec<DBIterator>>> =
    LazyLock::new(|| Mutex::new(Vec::with_capacity(*POOL_CAPACITY)));

/// Take a DBIterator from the global pool, or return None.
fn pool_take() -> Option<DBIterator> {
    GLOBAL_ITER_POOL.lock().pop()
}

/// Return a DBIterator to the global pool for reuse.
/// The iterator's sources are cleared to release `Arc<TableReader>` references,
/// preventing stale SST files from being kept alive by pooled iterators.
pub fn pool_return(mut iter: DBIterator) {
    iter.reset(Vec::new(), 0);
    let mut pool = GLOBAL_ITER_POOL.lock();
    if pool.len() < *POOL_CAPACITY {
        pool.push(iter);
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
        self.snapshots.lock().retain(|&s| s != seq);
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
    sst_number: u64,
    sst_path: std::path::PathBuf,
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

/// The core database handle.
pub struct DB {
    path: PathBuf,
    options: DbOptions,
    inner: Arc<Mutex<DBInner>>,
    /// Global sequence number (next to assign).
    sequence: AtomicU64,
    /// Write queue and condvar for group commit.
    write_queue: Mutex<WriteQueueState>,
    write_cv: Condvar,
    closed: AtomicBool,
    /// Fast-check flag for background errors (avoids Mutex lock on every read).
    has_bg_error: Arc<AtomicBool>,
    /// Background error (e.g. WAL sync failure). Once set, all subsequent
    /// operations are rejected. Models RocksDB's background error state.
    bg_error: Arc<Mutex<Option<String>>>,
    block_cache: Arc<BlockCache>,
    table_cache: Arc<TableCache>,
    rate_limiter: Arc<RateLimiter>,
    stats: Arc<DbStats>,
    /// Shutdown flag for compaction threads.
    compaction_shutdown: Arc<AtomicBool>,
    /// Notification condvar for compaction threads: (has_work, condvar).
    compaction_notify: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    /// Background compaction thread handles.
    compaction_handles: Mutex<Vec<JoinHandle<()>>>,
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
    /// Explicitly taken in `simulate_crash()` to allow re-open in tests.
    /// The field is "read" via Drop (releases flock) — not dead code.
    #[allow(dead_code)]
    lock_file: Option<std::fs::File>,
}

// SAFETY: WriteRequest pointers are only accessed under write_queue lock.
unsafe impl Send for DB {}
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

impl DB {
    /// Open or create a database.
    pub fn open(options: DbOptions, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if options.create_if_missing {
            std::fs::create_dir_all(&path).c(d!())?;
        } else if !path.exists() {
            return Err(eg!(Error::InvalidArgument(format!(
                "DB path does not exist: {}",
                path.display()
            ))));
        }

        if options.error_if_exists {
            let current = path.join("CURRENT");
            if current.exists() {
                return Err(eg!(Error::InvalidArgument(format!(
                    "DB already exists: {}",
                    path.display()
                ))));
            }
        }

        // Acquire exclusive directory lock to prevent concurrent DB access
        let lock_file = {
            let lock_path = path.join("LOCK");
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&lock_path)
                .c(d!())?;
            #[cfg(unix)]
            {
                use std::os::fd::AsRawFd;
                let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                if ret != 0 {
                    let err = std::io::Error::last_os_error();
                    return Err(eg!(Error::InvalidArgument(format!(
                        "failed to lock DB directory {}: {} (is another process using it?)",
                        path.display(),
                        err
                    ))));
                }
            }
            Some(file)
        };

        // Create caches and infra
        let block_cache = Arc::new(BlockCache::new(options.block_cache_capacity));
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
                .c(d!())?;
        let mut max_sequence = versions.last_sequence();

        // Recover from any WAL files not yet flushed
        let mut active_memtable = Arc::new(MemTable::new());
        let mut wal_numbers: Vec<u64> = Vec::new();
        for entry in std::fs::read_dir(&path).c(d!())? {
            let entry = entry.c(d!())?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
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

        for wal_num in &wal_numbers {
            let wal_path = path.join(format!("{:06}.wal", wal_num));
            if let Ok(mut reader) = WalReader::new(&wal_path) {
                for record in reader.iter() {
                    match record {
                        Ok(data) => {
                            Self::replay_wal_record(&data, &active_memtable, &mut max_sequence)
                                .c(d!())?;
                        }
                        Err(e) => {
                            tracing::warn!("WAL {} recovery error: {}", wal_num, e);
                            break;
                        }
                    }
                }
            }
        }

        // If we recovered data from WALs, flush it to SST before deleting
        // the old WALs. This ensures the data persists even if we crash again
        // before writing to the new WAL.
        if !wal_numbers.is_empty() && active_memtable.approximate_size() > 0 {
            let sst_number = versions.new_file_number();
            let sst_path = path.join(format!("{:06}.sst", sst_number));
            let build_opts = TableBuildOptions {
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
            let mut builder = TableBuilder::new(&sst_path, build_opts).c(d!())?;
            for (key, value) in active_memtable.iter() {
                builder.add(&key, &value).c(d!())?;
            }
            let build_result = builder.finish().c(d!())?;

            let mut edit = VersionEdit::new();
            edit.set_last_sequence(max_sequence);
            edit.set_next_file_number(versions.next_file_number());
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
            versions.log_and_apply(edit).c(d!())?;

            // Reset the memtable — data is now safely in SST
            active_memtable = Arc::new(MemTable::new());
        }

        // Create a fresh WAL
        let wal_number = versions.new_file_number();
        let wal_path = path.join(format!("{:06}.wal", wal_number));
        let wal_writer = WalWriter::new(&wal_path).c(d!())?;

        // Safe to clean up old WAL files now — data is in SST
        for wal_num in &wal_numbers {
            let old_wal = path.join(format!("{:06}.wal", wal_num));
            if let Err(e) = std::fs::remove_file(&old_wal) {
                tracing::warn!("failed to remove old WAL {}: {}", old_wal.display(), e);
            }
        }

        // Record the new log number in MANIFEST
        {
            let mut edit = VersionEdit::new();
            edit.set_log_number(wal_number);
            edit.set_next_file_number(versions.next_file_number());
            edit.set_last_sequence(max_sequence);
            versions.log_and_apply(edit).c(d!())?;
        }

        let sequence_start = max_sequence + 1;

        let inner = Arc::new(Mutex::new(DBInner {
            active_memtable,
            immutable_memtables: Vec::new(),
            wal_writer: Some(wal_writer),
            wal_number,
            versions,
        }));

        // Spawn background compaction threads
        let compaction_shutdown = Arc::new(AtomicBool::new(false));
        let compaction_notify = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let has_bg_error = Arc::new(AtomicBool::new(false));
        let bg_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
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

            let handle = thread::Builder::new()
                .name(format!("mmdb-compaction-{}", i))
                .spawn(move || {
                    // Helper: set background error and log it.
                    let set_error = |msg: String| {
                        tracing::error!("{}", msg);
                        bg_has_error.store(true, Ordering::Release);
                        *bg_error_msg.lock() = Some(msg);
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
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || -> std::result::Result<(), String> {
                                // Drain read-compaction hints (lock released during I/O)
                                {
                                    let hints: Vec<CompactionHint> =
                                        std::mem::take(&mut *bg_hints.lock());
                                    if !hints.is_empty() {
                                        let active_snaps = bg_snapshot_list.as_sorted_vec();
                                        for hint in &hints {
                                            // Phase 1: pick + pre-allocate (short lock)
                                            let pick = {
                                                let mut inner = bg_inner.lock();
                                                let version = inner.versions.current();
                                                LeveledCompaction::pick_compaction_for_hint(
                                                    &version, hint,
                                                )
                                                .map(|task| {
                                                    let max_out = task.total_input_size()
                                                        / bg_options.target_file_size_base
                                                        + bg_options.max_subcompactions.max(1)
                                                            as u64;
                                                    let file_start = inner
                                                        .versions
                                                        .reserve_file_numbers(max_out);
                                                    (task, file_start)
                                                })
                                            }; // lock released

                                            if let Some((task, file_start)) = pick {
                                                // Phase 2: I/O (no lock)
                                                let output =
                                                    LeveledCompaction::execute_compaction_io(
                                                        &task,
                                                        file_start,
                                                        &bg_path,
                                                        &bg_options,
                                                        Some(&bg_rate_limiter),
                                                        Some(&bg_stats),
                                                        &active_snaps,
                                                    )
                                                    .map_err(|e| {
                                                        format!("hint compaction error: {}", e)
                                                    })?;

                                                // Phase 3: install (short lock)
                                                let mut inner = bg_inner.lock();
                                                LeveledCompaction::install_compaction(
                                                    output,
                                                    &mut inner.versions,
                                                    Some(&bg_table_cache),
                                                    &bg_path,
                                                    Some(&bg_stats),
                                                )
                                                .map_err(|e| {
                                                    format!("hint compaction install error: {}", e)
                                                })?;
                                                bg_l0_count.store(
                                                    inner.versions.current().l0_file_count(),
                                                    Ordering::Relaxed,
                                                );
                                                refresh_super_version(&bg_sv, &inner);
                                            }
                                        }
                                    }
                                }

                                // Run pending compactions (lock released during I/O)
                                loop {
                                    let active_snaps = bg_snapshot_list.as_sorted_vec();

                                    // Phase 1: pick + pre-allocate (short lock)
                                    let pick = {
                                        let mut inner = bg_inner.lock();
                                        let version = inner.versions.current();
                                        match LeveledCompaction::pick_compaction(
                                            &version,
                                            &bg_options,
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
                                                let max_out = task.total_input_size()
                                                    / bg_options.target_file_size_base
                                                    + bg_options.max_subcompactions.max(1) as u64;
                                                let file_start =
                                                    inner.versions.reserve_file_numbers(max_out);
                                                Some((task, file_start, l0_inputs))
                                            }
                                            None => None,
                                        }
                                    }; // lock released

                                    let Some((task, file_start, l0_inputs)) = pick else {
                                        break;
                                    };

                                    // Phase 2: I/O (no lock held)
                                    let output = LeveledCompaction::execute_compaction_io(
                                        &task,
                                        file_start,
                                        &bg_path,
                                        &bg_options,
                                        Some(&bg_rate_limiter),
                                        Some(&bg_stats),
                                        &active_snaps,
                                    )
                                    .map_err(|e| format!("compaction error: {}", e))?;

                                    // Phase 3: install (short lock)
                                    {
                                        let mut inner = bg_inner.lock();
                                        LeveledCompaction::install_compaction(
                                            output,
                                            &mut inner.versions,
                                            Some(&bg_table_cache),
                                            &bg_path,
                                            Some(&bg_stats),
                                        )
                                        .map_err(|e| format!("compaction install error: {}", e))?;
                                        for num in &l0_inputs {
                                            bg_block_cache.unpin_file(*num);
                                        }
                                        bg_l0_count.store(
                                            inner.versions.current().l0_file_count(),
                                            Ordering::Relaxed,
                                        );
                                        refresh_super_version(&bg_sv, &inner);
                                    }
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
                .map_err(|e| eg!("failed to spawn compaction thread: {}", e))?;
            compaction_handles.push(handle);
        }

        let db = Self {
            path,
            options,
            inner,
            sequence: AtomicU64::new(sequence_start),
            write_queue: Mutex::new(WriteQueueState {
                queue: VecDeque::new(),
                leader_active: false,
            }),
            write_cv: Condvar::new(),
            closed: AtomicBool::new(false),
            has_bg_error,
            bg_error,
            block_cache,
            table_cache,
            rate_limiter,
            stats,
            compaction_shutdown,
            compaction_notify,
            compaction_handles: Mutex::new(compaction_handles),
            l0_file_count,
            super_version,
            read_compaction_hints,
            read_counter: AtomicU64::new(0),
            snapshot_list,
            lock_file,
        };

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
        self.check_usable().c(d!())?;
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_batch_inner(batch, write_options)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_with_options(&WriteOptions::default(), key)
    }

    pub fn delete_with_options(&self, write_options: &WriteOptions, key: &[u8]) -> Result<()> {
        self.check_usable().c(d!())?;
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
        self.check_usable().c(d!())?;
        let mut batch = WriteBatch::new();
        batch.delete_range(begin, end);
        self.write_batch_inner(batch, write_options)
    }

    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.write_with_options(batch, &WriteOptions::default())
    }

    pub fn write_with_options(
        &self,
        batch: WriteBatch,
        write_options: &WriteOptions,
    ) -> Result<()> {
        self.check_usable().c(d!())?;
        if batch.is_empty() {
            return Ok(());
        }
        self.write_batch_inner(batch, write_options)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let result = self
            .get_with_options(&ReadOptions::default(), key)
            .c(d!())?;
        if let Some(ref v) = result {
            self.stats.record_read(key.len() as u64 + v.len() as u64);
        }
        Ok(result)
    }

    pub fn get_with_options(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_usable().c(d!())?;

        let seq = match options.snapshot {
            Some(s) => s,
            None => self.current_sequence(),
        };

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
            return Ok(result);
        }

        // 2. Immutable MemTables (newest first)
        for imm in imm_mems {
            if let Some((result, entry_seq)) = imm.get_with_seq(key, seq) {
                if max_tomb_seq > entry_seq {
                    return Ok(None);
                }
                return Ok(result);
            }
        }

        // 3. If no point entry in memtables but a tombstone covers the key,
        // the key is deleted (tombstone covers entries in older SSTs too).
        if max_tomb_seq > 0 {
            return Ok(None);
        }

        // 4. L0 SST files (newest first, may overlap)
        // Check range tombstones in each file and accumulate max_tomb_seq.
        for tf in version.level_files(0) {
            if tf.meta.has_range_deletions {
                let file_tomb_seq = tf.reader.max_covering_tombstone_seq(key, seq).c(d!())?;
                max_tomb_seq = max_tomb_seq.max(file_tomb_seq);
            }
            if let Some((result, entry_seq)) = tf.reader.get_internal_with_seq(key, seq).c(d!())? {
                if max_tomb_seq > entry_seq {
                    return Ok(None);
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
            // Tombstones can span beyond their containing file's key range.
            for tf in files {
                if tf.meta.has_range_deletions {
                    let file_tomb_seq = tf.reader.max_covering_tombstone_seq(key, seq).c(d!())?;
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
                && let Some((result, entry_seq)) =
                    tf.reader.get_internal_with_seq(key, seq).c(d!())?
            {
                if max_tomb_seq > entry_seq {
                    return Ok(None);
                }
                // Sample reads at level >= 2 for read-triggered compaction.
                if level >= 2 {
                    self.stats.maybe_sample_read_level(level);
                    self.maybe_check_read_compaction();
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
        self.check_usable().c(d!())?;

        let seq = match options.snapshot {
            Some(s) => s,
            None => self.current_sequence(),
        };

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
            sources.push(IterSource::from_memtable(cursor));
        }

        // Immutable memtables
        for imm in imm_mems {
            if imm.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(Arc::clone(imm));
            sources.push(IterSource::from_memtable(cursor));
        }

        // SST files — only include files whose key range overlaps [lower_bound, upper_bound].
        // L0 files can overlap each other so we check each individually.
        // L1+ files are sorted and non-overlapping — use a single LevelIterator per level.
        for tf in version.level_files(0) {
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
            if tf.meta.has_range_deletions {
                any_range_deletions = true;
            }
            let iter = if options.block_property_filters.is_empty() {
                TableIterator::new(tf.reader.clone())
            } else {
                TableIterator::new(tf.reader.clone())
                    .with_block_filters(options.block_property_filters.clone())
            };
            sources.push(IterSource::from_table_iter(iter));
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
            let mut level_iter = LevelIterator::new(files.to_vec()).with_range_hints(
                lower_bound.map(|s| s.to_vec()),
                upper_bound.map(|e| e.to_vec()),
            );
            if !options.block_property_filters.is_empty() {
                level_iter = level_iter.with_block_filters(options.block_property_filters.clone());
            }
            sources.push(IterSource::from_level_iter(level_iter));
        }

        let mut db_iter = match pool_take() {
            Some(mut pooled) => {
                pooled.reset(sources, seq);
                pooled
            }
            None => DBIterator::from_sources(sources, seq),
        };

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
            // A tombstone from level L can only delete keys from levels > L.
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
                    let ts = tf.reader.get_range_tombstones().c(d!())?;
                    for (b, e, s) in ts {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
            for level in 1..version.num_levels {
                for tf in version.level_files(level) {
                    if tf.meta.has_range_deletions {
                        let ts = tf.reader.get_range_tombstones().c(d!())?;
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
    ///    whose per-block bloom filters report that `prefix` is absent.
    ///
    /// For cross-prefix ranges (e.g. `[b"m", b"z")`) use `iter_with_range()`
    /// instead, as there is no single prefix that covers the query.
    ///
    /// # Sub-range within a prefix
    ///
    /// Set `ReadOptions::iterate_lower_bound` / `iterate_upper_bound` to further
    /// restrict iteration to a sub-range inside the prefix.
    pub fn iter_with_prefix(&self, prefix: &[u8], options: &ReadOptions) -> Result<DBIterator> {
        let seq = options.snapshot.unwrap_or_else(|| self.current_sequence());
        self.iter_with_prefix_inner(prefix, seq, options)
    }

    /// Create a prefix-bounded iterator at a specific sequence number.
    fn iter_with_prefix_inner(
        &self,
        prefix: &[u8],
        seq: SequenceNumber,
        options: &ReadOptions,
    ) -> Result<DBIterator> {
        self.check_usable().c(d!())?;

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
            sources.push(IterSource::from_memtable(cursor));
        }

        // Immutable memtables
        for imm in imm_mems {
            if imm.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(Arc::clone(imm));
            sources.push(IterSource::from_memtable(cursor));
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
            if tf.meta.has_range_deletions {
                any_range_deletions = true;
            }
            let iter = TableIterator::new(tf.reader.clone());
            sources.push(IterSource::from_table_iter(iter));
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
            let level_iter = LevelIterator::new(files.to_vec())
                .with_prefix(prefix_owned.to_vec())
                .with_range_hints(Some(prefix_owned.to_vec()), prefix_upper.clone());
            sources.push(IterSource::from_level_iter(level_iter));
        }

        let mut iter = match pool_take() {
            Some(mut pooled) => {
                pooled.reset_with_prefix(sources, seq, prefix_owned.to_vec());
                pooled
            }
            None => DBIterator::from_sources_with_prefix(sources, seq, prefix_owned.to_vec()),
        };

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
                    let ts = tf.reader.get_range_tombstones().c(d!())?;
                    for (b, e, s) in ts {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
            for level in 1..version.num_levels {
                for tf in version.level_files(level) {
                    if tf.meta.has_range_deletions {
                        let ts = tf.reader.get_range_tombstones().c(d!())?;
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
    /// current DB state. Batch entries appear with sequence numbers above the
    /// current snapshot so they take precedence over existing data.
    pub fn iter_with_batch(&self, batch: &WriteBatchWithIndex) -> Result<DBIterator> {
        self.check_usable().c(d!())?;

        let seq = self.current_sequence();
        let batch_entries = batch.sorted_entries(seq + 1);
        let batch_count = batch.operation_count();

        // Lock-free read via SuperVersion.
        let sv = self.get_super_version();
        let (active_mem, imm_mems, version) =
            (&sv.active_memtable, &sv.immutable_memtables, &sv.version);

        let est_sources = 2 + imm_mems.len() + version.level_files(0).len() + version.num_levels;
        let mut sources: Vec<IterSource> = Vec::with_capacity(est_sources);

        // Batch source first (highest priority due to higher sequence numbers).
        sources.push(IterSource::new(batch_entries));

        // Active memtable.
        {
            let cursor = MemTableCursorIter::new(active_mem.clone());
            sources.push(IterSource::from_memtable(cursor));
        }

        // Immutable memtables.
        for imm in imm_mems.iter() {
            let cursor = MemTableCursorIter::new(Arc::clone(imm));
            sources.push(IterSource::from_memtable(cursor));
        }

        // SST files: L0 individually, L1+ via LevelIterator.
        for tf in version.level_files(0) {
            let iter = TableIterator::new(tf.reader.clone());
            sources.push(IterSource::from_table_iter(iter));
        }
        for level in 1..version.num_levels {
            let files = version.level_files(level);
            if files.is_empty() {
                continue;
            }
            let level_iter = LevelIterator::new(files.to_vec());
            sources.push(IterSource::from_level_iter(level_iter));
        }

        let mut db_iter = DBIterator::from_sources(sources, seq + batch_count);

        // Collect range tombstones (same as iter_with_range).
        let mut all_tombstones: Vec<(Vec<u8>, Vec<u8>, u64, usize)> = Vec::new();
        // Batch range tombstones use position-based sequences (preserving write order).
        for &(ref b, ref e, pos) in batch.range_tombstones() {
            all_tombstones.push((b.clone(), e.clone(), seq + 1 + pos, 0));
        }
        if active_mem.has_range_deletions() {
            for (b, e, s) in active_mem.get_range_tombstones() {
                all_tombstones.push((b, e, s, 0));
            }
        }
        for imm in imm_mems.iter() {
            if imm.has_range_deletions() {
                for (b, e, s) in imm.get_range_tombstones() {
                    all_tombstones.push((b, e, s, 0));
                }
            }
        }
        for tf in version.level_files(0) {
            if tf.meta.has_range_deletions {
                let ts = tf.reader.get_range_tombstones().c(d!())?;
                for (b, e, s) in ts {
                    all_tombstones.push((b, e, s, 0));
                }
            }
        }
        for level in 1..version.num_levels {
            for tf in version.level_files(level) {
                if tf.meta.has_range_deletions {
                    let ts = tf.reader.get_range_tombstones().c(d!())?;
                    for (b, e, s) in ts {
                        all_tombstones.push((b, e, s, level));
                    }
                }
            }
        }
        if !all_tombstones.is_empty() {
            db_iter.set_range_tombstones_with_levels(all_tombstones);
        }

        Ok(db_iter)
    }

    pub fn snapshot_seq(&self) -> SequenceNumber {
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
    pub fn release_snapshot(&self, seq: SequenceNumber) {
        self.snapshot_list.release(seq);
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get a database property.
    ///
    /// Supported properties:
    /// - `"num-files-at-level{N}"` — number of SST files at level N
    /// - `"total-sst-size"` — total size of all SST files in bytes
    /// - `"block-cache-usage"` — approximate block cache entry count
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
                let needed = LeveledCompaction::pick_compaction(&version, &self.options).is_some();
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
    pub fn flush(&self) -> Result<()> {
        self.check_usable().c(d!())?;
        let _wg = self.write_queue.lock(); // serialize with writers
        let mut inner = self.inner.lock();
        if inner.active_memtable.is_empty() {
            return Ok(());
        }
        let frozen = self.freeze_memtable(&mut inner).c(d!())?;
        drop(inner); // release lock during SST write
        let build_result = self.flush_frozen_memtable(&frozen).c(d!())?;
        let mut inner = self.inner.lock(); // reacquire
        self.install_flush(&mut inner, &frozen, build_result)
            .c(d!())?;
        self.maybe_compact(&mut inner).c(d!())?;
        Ok(())
    }

    /// Run compaction if needed (L0 → L1 or Ln → Ln+1).
    /// Runs inline while holding locks for deterministic behavior.
    pub fn compact(&self) -> Result<()> {
        self.check_usable().c(d!())?;
        let _wg = self.write_queue.lock();
        let mut inner = self.inner.lock();
        self.do_compaction(&mut inner)
    }

    /// Compact all keys in the given range across all levels.
    /// If `begin` is None, starts from the beginning. If `end` is None, goes to the end.
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.check_usable().c(d!())?;

        // First flush memtable to ensure all data is in SSTs
        {
            let _wg = self.write_queue.lock();
            let mut inner = self.inner.lock();
            if !inner.active_memtable.is_empty() {
                let frozen = self.freeze_memtable(&mut inner).c(d!())?;
                drop(inner);
                let build_result = self.flush_frozen_memtable(&frozen).c(d!())?;
                let mut inner = self.inner.lock();
                self.install_flush(&mut inner, &frozen, build_result)
                    .c(d!())?;
            }
        }

        // Compact files overlapping the specified range
        let _wg = self.write_queue.lock();
        let mut inner = self.inner.lock();

        // If no range specified, fall back to full compaction
        if begin.is_none() && end.is_none() {
            return self.do_compaction(&mut inner);
        }

        // Range-filtered compaction: compact files overlapping [begin, end)
        let active_snaps = self.snapshot_list.as_sorted_vec();
        loop {
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
                    LeveledCompaction::execute_compaction_with_cache(
                        &task,
                        &mut inner.versions,
                        &self.path,
                        &self.options,
                        Some(&self.table_cache),
                        Some(&self.rate_limiter),
                        Some(&self.stats),
                        &active_snaps,
                    )
                    .c(d!())?;
                    for num in &l0_inputs {
                        self.block_cache.unpin_file(*num);
                    }
                }
                None => break,
            }
        }
        // Update cached L0 count and install new SuperVersion so readers
        // see the compacted state immediately.
        self.l0_file_count
            .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
        self.install_super_version(&inner);
        Ok(())
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

        let mut first_error: Option<Box<dyn ruc::err::RucError>> = None;

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
        self.has_bg_error.store(true, Ordering::Release);
        *self.bg_error.lock() = Some(msg);
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
                hints.push(CompactionHint {
                    level,
                    read_count: count,
                });
            }
        }
        if !hints.is_empty() {
            self.signal_compaction();
        }
    }

    fn check_usable(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(eg!(Error::DbClosed));
        }
        // Fast path: only lock if the error flag is set.
        if self.has_bg_error.load(Ordering::Acquire)
            && let Some(ref msg) = *self.bg_error.lock()
        {
            return Err(eg!(Error::BackgroundError(msg.clone())));
        }
        Ok(())
    }

    fn current_sequence(&self) -> SequenceNumber {
        self.sequence.load(Ordering::Acquire).saturating_sub(1)
    }

    /// Apply write backpressure based on L0 file count.
    fn maybe_throttle_writes(&self) {
        // Fast path: check cached L0 count without locking inner.
        let l0_count = self.l0_file_count.load(Ordering::Relaxed);

        if l0_count >= self.options.l0_stop_trigger {
            // Slow path: lock inner and do inline compaction.
            let mut inner = self.inner.lock();
            while inner.versions.current().l0_file_count() >= self.options.l0_stop_trigger {
                let _ = self.do_compaction(&mut inner);
                if inner.versions.current().l0_file_count() >= self.options.l0_stop_trigger {
                    break; // Can't compact further
                }
            }
            self.l0_file_count
                .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
        } else if l0_count >= self.options.l0_slowdown_trigger {
            // Progressive delay: more L0 files → longer sleep
            let delay_us = (l0_count - self.options.l0_slowdown_trigger + 1) as u64 * 1000;
            thread::sleep(Duration::from_micros(delay_us));
        }
    }

    fn write_batch_inner(&self, batch: WriteBatch, write_options: &WriteOptions) -> Result<()> {
        if write_options.no_slowdown {
            let l0_count = self.l0_file_count.load(Ordering::Relaxed);
            if l0_count >= self.options.l0_slowdown_trigger {
                return Err(eg!(Error::InvalidArgument(
                    "write stalled: no_slowdown is set".to_string(),
                )));
            }
        } else {
            self.maybe_throttle_writes();
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
                let r = unsafe { &mut *rp };
                if r.result.is_none() {
                    r.result = match &result {
                        Ok(()) => Some(Ok(())),
                        Err(e) => {
                            Some(Err(eg!(Error::Io(std::io::Error::other(format!("{}", e))))))
                        }
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

    /// Process a batch group: write WAL, apply to memtable, sync, flush if needed.
    /// Returns Ok(()) if all batches succeeded. On WAL add_record failure,
    /// sets result on each request (Ok for succeeded, Err for failed+later).
    fn write_batch_group(&self, batch_group: &[*mut WriteRequest]) -> Result<()> {
        let mut need_sync = false;
        let mut inner = self.inner.lock();
        let mut wal_add_error: Option<(Box<dyn ruc::RucError>, usize)> = None;

        for (batch_idx, &req_ptr) in batch_group.iter().enumerate() {
            let r = unsafe { &mut *req_ptr };
            let first_seq = self
                .sequence
                .fetch_add(r.batch.len() as u64, Ordering::AcqRel);
            need_sync |= r.sync;

            if !r.disable_wal {
                let wal_record = Self::encode_wal_record(first_seq, &r.batch);
                if let Some(ref mut wal) = inner.wal_writer
                    && let Err(e) = wal.add_record(&wal_record)
                {
                    wal_add_error = Some((e, batch_idx));
                    break;
                }
            }

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
            self.stats.record_write(batch_bytes);
        }

        if let Some((e, fail_idx)) = wal_add_error {
            // Flush already-buffered WAL records for succeeded batches so
            // their data is durable even though later batches failed.
            let flush_ok = if fail_idx > 0 {
                if let Some(ref mut wal) = inner.wal_writer {
                    wal.flush().is_ok()
                } else {
                    false
                }
            } else {
                false
            };

            if flush_ok {
                // Flush succeeded: earlier batches are durable
                for &rp in &batch_group[..fail_idx] {
                    let rr = unsafe { &mut *rp };
                    rr.result = Some(Ok(()));
                }
            } else if fail_idx > 0 {
                // Flush failed: earlier batches are NOT durable either
                for &rp in &batch_group[..fail_idx] {
                    let rr = unsafe { &mut *rp };
                    rr.result = Some(Err(eg!(Error::Io(std::io::Error::other(
                        "WAL flush failed after partial batch group write"
                    )))));
                }
            }
            for &rp in &batch_group[fail_idx..] {
                let rr = unsafe { &mut *rp };
                rr.result = Some(Err(eg!(Error::Io(std::io::Error::other(format!("{}", e))))));
            }
            return Err(eg!(Error::Io(std::io::Error::other(format!("{}", e)))));
        }

        // Single fsync for all batches
        let any_wal = batch_group.iter().any(|&p| !unsafe { &*p }.disable_wal);
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
                let rr = unsafe { &mut *rp };
                rr.result = Some(Err(eg!(Error::Io(std::io::Error::other(format!("{}", e))))));
            }
            return Err(eg!(Error::Io(std::io::Error::other(format!("{}", e)))));
        }

        // Check memtable size threshold — release lock during SST I/O
        let mut did_flush = false;
        if inner.active_memtable.approximate_size() >= self.options.write_buffer_size {
            match self.freeze_memtable(&mut inner) {
                Ok(frozen) => {
                    drop(inner); // release lock for slow SST write
                    match self.flush_frozen_memtable(&frozen) {
                        Ok(build_result) => {
                            let mut inner = self.inner.lock(); // reacquire
                            match self.install_flush(&mut inner, &frozen, build_result) {
                                Ok(()) => did_flush = true,
                                Err(e) => tracing::error!("flush install failed: {}", e),
                            }
                            drop(inner);
                        }
                        Err(e) => {
                            tracing::error!("auto-flush SST write failed (data safe in WAL): {}", e)
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("auto-flush freeze failed: {}", e);
                    drop(inner);
                }
            }
        } else {
            drop(inner);
        }

        if did_flush {
            self.signal_compaction();
        }

        Ok(())
    }

    /// Freeze+flush in one call (holds lock throughout).
    /// Used by `close()` and `do_compaction` where lock is already held
    /// and releasing it would complicate the caller.
    fn freeze_and_flush(&self, inner: &mut DBInner) -> Result<()> {
        let frozen = self.freeze_memtable(inner).c(d!())?;
        let build_result = self.flush_frozen_memtable(&frozen).c(d!())?;
        self.install_flush(inner, &frozen, build_result).c(d!())?;
        Ok(())
    }

    /// Phase 1 (under lock, fast): swap memtable, create WAL, allocate SST number.
    fn freeze_memtable(&self, inner: &mut DBInner) -> Result<FrozenMemtable> {
        let old_mem = std::mem::replace(&mut inner.active_memtable, Arc::new(MemTable::new()));

        let new_wal_number = inner.versions.new_file_number();
        let new_wal_path = self.path.join(format!("{:06}.wal", new_wal_number));
        let new_wal = WalWriter::new(&new_wal_path).c(d!())?;
        let old_wal_number = inner.wal_number;
        inner.wal_writer = Some(new_wal);
        inner.wal_number = new_wal_number;

        let sst_number = inner.versions.new_file_number();
        let sst_path = self.path.join(format!("{:06}.sst", sst_number));

        Ok(FrozenMemtable {
            old_mem,
            sst_number,
            sst_path,
            old_wal_number,
            new_wal_number,
        })
    }

    /// Phase 2 (no lock needed, slow I/O): write SST from frozen memtable.
    fn flush_frozen_memtable(
        &self,
        frozen: &FrozenMemtable,
    ) -> Result<crate::sst::table_builder::TableBuildResult> {
        self.write_memtable_to_sst(&frozen.old_mem, &frozen.sst_path)
    }

    /// Phase 3 (under lock, fast): install flush result into version.
    fn install_flush(
        &self,
        inner: &mut DBInner,
        frozen: &FrozenMemtable,
        build_result: crate::sst::table_builder::TableBuildResult,
    ) -> Result<()> {
        let mut edit = VersionEdit::new();
        edit.set_log_number(frozen.new_wal_number);
        edit.set_next_file_number(inner.versions.next_file_number());
        edit.set_last_sequence(self.current_sequence());
        edit.add_file(
            0, // L0
            FileMetaData {
                number: frozen.sst_number,
                file_size: build_result.file_size,
                smallest_key: build_result.smallest_key.unwrap_or_default(),
                largest_key: build_result.largest_key.unwrap_or_default(),
                has_range_deletions: build_result.has_range_deletions,
            },
        );
        inner.versions.log_and_apply(edit).c(d!())?;
        self.stats.record_flush();

        if self.options.pin_l0_filter_and_index_blocks_in_cache {
            let version = inner.versions.current();
            for tf in version.level_files(0) {
                if tf.meta.number == frozen.sst_number {
                    tf.reader.pin_metadata_in_cache();
                    break;
                }
            }
        }

        self.l0_file_count
            .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
        self.install_super_version(inner);

        let old_wal_path = self.path.join(format!("{:06}.wal", frozen.old_wal_number));
        if let Err(e) = std::fs::remove_file(&old_wal_path) {
            tracing::warn!("failed to remove old WAL {}: {}", old_wal_path.display(), e);
        }

        Ok(())
    }

    fn maybe_compact(&self, inner: &mut DBInner) -> Result<()> {
        let version = inner.versions.current();
        if version.l0_file_count() >= self.options.l0_compaction_trigger {
            self.do_compaction(inner).c(d!())?;
        }
        Ok(())
    }

    fn do_compaction(&self, inner: &mut DBInner) -> Result<()> {
        // Force-compact: use trigger=1 to compact any L0 files.
        let force_opts = DbOptions {
            l0_compaction_trigger: 1,
            ..self.options.clone()
        };
        let active_snaps = self.snapshot_list.as_sorted_vec();
        loop {
            let version = inner.versions.current();
            match LeveledCompaction::pick_compaction(&version, &force_opts) {
                Some(task) => {
                    // Collect L0 input files for cache unpinning after compaction
                    let l0_inputs: Vec<u64> = if task.level == 0 {
                        task.input_files_level
                            .iter()
                            .map(|f| f.meta.number)
                            .collect()
                    } else {
                        Vec::new()
                    };
                    LeveledCompaction::execute_compaction_with_cache(
                        &task,
                        &mut inner.versions,
                        &self.path,
                        &self.options,
                        Some(&self.table_cache),
                        Some(&self.rate_limiter),
                        Some(&self.stats),
                        &active_snaps,
                    )
                    .c(d!())?;
                    // Unpin L0 files from block cache after compaction
                    for num in &l0_inputs {
                        self.block_cache.unpin_file(*num);
                    }
                }
                None => break,
            }
        }
        // Force-merge levels that have multiple files (space reclamation).
        // With background compaction, tombstones may end up in separate files
        // from the data they cover. Merging fixes this.
        for level in 1..self.options.num_levels {
            LeveledCompaction::force_merge_level(
                level,
                &mut inner.versions,
                &self.path,
                &self.options,
                Some(&self.table_cache),
                Some(&self.rate_limiter),
                Some(&self.stats),
                &active_snaps,
            )
            .c(d!())?;
        }
        // Update cached L0 count after compaction
        self.l0_file_count
            .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
        self.install_super_version(inner);
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

    fn replay_wal_record(data: &[u8], mem: &MemTable, max_sequence: &mut u64) -> Result<()> {
        if data.len() < 12 {
            return Err(eg!("WAL record too short: {} bytes", data.len()));
        }
        let seq = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let count = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let mut offset = 12;

        for i in 0..count {
            if offset >= data.len() {
                return Err(eg!("WAL record truncated at entry {}/{}", i, count));
            }
            let entry_seq = seq + i as u64;
            *max_sequence = (*max_sequence).max(entry_seq);

            let vt = data[offset];
            offset += 1;
            if offset + 4 > data.len() {
                return Err(eg!(
                    "WAL record truncated reading key length at entry {}",
                    i
                ));
            }
            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + key_len > data.len() {
                return Err(eg!("WAL record truncated reading key at entry {}", i));
            }
            let key = &data[offset..offset + key_len];
            offset += key_len;

            match ValueType::from_u8(vt) {
                Some(ValueType::Value) => {
                    if offset + 4 > data.len() {
                        return Err(eg!(
                            "WAL record truncated reading value length at entry {}",
                            i
                        ));
                    }
                    let val_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + val_len > data.len() {
                        return Err(eg!("WAL record truncated reading value at entry {}", i));
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
                        return Err(eg!(
                            "WAL record truncated reading range-del end key length at entry {}",
                            i
                        ));
                    }
                    let val_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + val_len > data.len() {
                        return Err(eg!(
                            "WAL record truncated reading range-del end key at entry {}",
                            i
                        ));
                    }
                    let value = &data[offset..offset + val_len];
                    offset += val_len;
                    mem.put(key, value, entry_seq, ValueType::RangeDeletion);
                }
                None => {
                    return Err(eg!(
                        "WAL record contains unknown value type {} at entry {}",
                        vt,
                        i
                    ));
                }
            }
        }
        Ok(())
    }

    fn write_memtable_to_sst(
        &self,
        mem: &MemTable,
        path: &Path,
    ) -> Result<crate::sst::table_builder::TableBuildResult> {
        let compression = if !self.options.compression_per_level.is_empty() {
            self.options.compression_per_level[0]
        } else {
            self.options.compression
        };
        let opts = TableBuildOptions {
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
        };

        let mut builder = TableBuilder::new(path, opts).c(d!())?;
        for (key, value) in mem.iter() {
            builder.add(&key, &value).c(d!())?;
        }
        builder.finish()
    }
}

/// Test utilities gated behind the `test-utils` feature.
#[cfg(any(test, feature = "test-utils"))]
impl DB {
    /// Simulate a crash: shut down background threads without flushing memtable.
    /// Useful for testing WAL recovery without zombie compaction threads.
    pub fn simulate_crash(mut self) {
        self.closed.store(true, Ordering::Release);
        self.compaction_shutdown.store(true, Ordering::Release);
        self.compaction_notify.1.notify_all();
        for handle in self.compaction_handles.lock().drain(..) {
            let _ = handle.join();
        }
        // Release directory lock before forgetting self, so subsequent
        // DB::open on the same directory will succeed (e.g., in tests).
        self.lock_file.take();
        std::mem::forget(self);
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
        // Shut down background compaction threads: set shutdown flag, wake all, join.
        self.compaction_shutdown.store(true, Ordering::Release);
        self.compaction_notify.1.notify_all();
        for handle in self.compaction_handles.lock().drain(..) {
            let _ = handle.join();
        }
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

        let sst_count = std::fs::read_dir(dir.path())
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
