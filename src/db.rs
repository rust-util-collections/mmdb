//! Core DB implementation with WAL, MemTable, SST, MANIFEST, and Iterator.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLock};

use crate::cache::block_cache::BlockCache;
use crate::cache::table_cache::TableCache;
use crate::compaction::LeveledCompaction;
use crate::error::{Error, Result};
use crate::iterator::db_iter::DBIterator;
use crate::iterator::merge::IterSource;
use crate::iterator::{BidiIterator, LevelIterator};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::manifest::version_set::VersionSet;
use crate::memtable::MemTable;
use crate::memtable::skiplist::MemTableCursorIter;
use crate::options::{DbOptions, ReadOptions, WriteOptions};
use crate::rate_limiter::RateLimiter;
use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
use crate::sst::table_reader::TableIterator;
use crate::stats::DbStats;
use crate::types::{self, SequenceNumber, ValueType, WriteBatch};
use crate::wal::{WalReader, WalWriter};
use ruc::*;

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
    /// Background error (e.g. WAL sync failure). Once set, all subsequent
    /// operations are rejected. Models RocksDB's background error state.
    bg_error: Mutex<Option<String>>,
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
    /// Readers take a read lock (uncontended, ~10ns). Writers update after
    /// memtable/version changes. Models RocksDB's SuperVersion mechanism.
    super_version: Arc<RwLock<Arc<SuperVersion>>>,
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
                            Self::replay_wal_record(&data, &active_memtable, &mut max_sequence);
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
            let _ = std::fs::remove_file(old_wal);
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
        let (l0_file_count, super_version) = {
            let g = inner.lock();
            let l0 = Arc::new(AtomicUsize::new(g.versions.current().l0_file_count()));
            let sv = Arc::new(RwLock::new(Arc::new(SuperVersion {
                active_memtable: g.active_memtable.clone(),
                immutable_memtables: g.immutable_memtables.clone(),
                version: g.versions.current(),
            })));
            (l0, sv)
        };
        let num_compaction_threads = options.max_background_compactions.max(1);
        let mut compaction_handles = Vec::with_capacity(num_compaction_threads);

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

            let handle = thread::Builder::new()
                .name(format!("mmdb-compaction-{}", i))
                .spawn(move || {
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
                        // Run pending compactions
                        loop {
                            let mut inner = bg_inner.lock();
                            let version = inner.versions.current();
                            match LeveledCompaction::pick_compaction(&version, &bg_options) {
                                Some(task) => {
                                    // Collect L0 input file numbers for cache unpinning
                                    let l0_inputs: Vec<u64> = if task.level == 0 {
                                        task.input_files_level
                                            .iter()
                                            .map(|f| f.meta.number)
                                            .collect()
                                    } else {
                                        Vec::new()
                                    };
                                    let _ = LeveledCompaction::execute_compaction_with_cache(
                                        &task,
                                        &mut inner.versions,
                                        &bg_path,
                                        &bg_options,
                                        Some(&bg_table_cache),
                                        Some(&bg_rate_limiter),
                                        Some(&bg_stats),
                                        &[],
                                    );
                                    // Unpin L0 files from block cache after compaction
                                    for num in &l0_inputs {
                                        bg_block_cache.unpin_file(*num);
                                    }
                                    // Update cached L0 count + SuperVersion
                                    bg_l0_count.store(
                                        inner.versions.current().l0_file_count(),
                                        Ordering::Relaxed,
                                    );
                                    *bg_sv.write() = Arc::new(SuperVersion {
                                        active_memtable: inner.active_memtable.clone(),
                                        immutable_memtables: inner.immutable_memtables.clone(),
                                        version: inner.versions.current(),
                                    });
                                }
                                None => break,
                            }
                        }
                    }
                })
                .expect("failed to spawn compaction thread");
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
            bg_error: Mutex::new(None),
            block_cache,
            table_cache,
            rate_limiter,
            stats,
            compaction_shutdown,
            compaction_notify,
            compaction_handles: Mutex::new(compaction_handles),
            l0_file_count,
            super_version,
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
        self.check_usable().c(d!())?;
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_batch_inner(batch, &WriteOptions::default())
    }

    /// Delete all keys in the range [begin, end).
    pub fn delete_range(&self, begin: &[u8], end: &[u8]) -> Result<()> {
        self.check_usable().c(d!())?;
        let mut batch = WriteBatch::new();
        batch.delete_range(begin, end);
        self.write_batch_inner(batch, &WriteOptions::default())
    }

    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.check_usable().c(d!())?;
        if batch.is_empty() {
            return Ok(());
        }
        self.write_batch_inner(batch, &WriteOptions::default())
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
                return Ok(result);
            }
        }

        // If a range tombstone was found in SSTs but no point entry, key is deleted
        if max_tomb_seq > 0 {
            return Ok(None);
        }

        Ok(None)
    }

    /// Create a forward iterator over the database.
    pub fn iter(&self) -> Result<DBIterator> {
        self.iter_with_options(&ReadOptions::default())
    }

    /// Create a forward iterator with options.
    ///
    /// Uses streaming TableIterators for SST files (O(1 block) memory per SST)
    /// instead of loading entire tables into memory.
    pub fn iter_with_options(&self, options: &ReadOptions) -> Result<DBIterator> {
        self.iter_with_range(options, None, None)
    }

    /// Create a forward iterator that only includes sources overlapping [start_hint, end_hint].
    /// SST files outside this range are skipped entirely, avoiding costly block reads.
    /// `None` bounds mean unbounded in that direction.
    pub fn iter_with_range(
        &self,
        options: &ReadOptions,
        start_hint: Option<&[u8]>,
        end_hint: Option<&[u8]>,
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

        let mut sources: Vec<IterSource> = Vec::new();
        let mut any_range_deletions = false;

        // Active memtable — cursor-based streaming iterator.
        {
            if active_mem.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(active_mem.clone());
            sources.push(IterSource::from_seekable(Box::new(cursor)));
        }

        // Immutable memtables
        for imm in imm_mems {
            if imm.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(Arc::clone(imm));
            sources.push(IterSource::from_seekable(Box::new(cursor)));
        }

        // SST files — only include files whose key range overlaps [start_hint, end_hint].
        // L0 files can overlap each other so we check each individually.
        // L1+ files are sorted and non-overlapping — use a single LevelIterator per level.
        for tf in version.level_files(0) {
            // Check if this file's key range overlaps the hint range.
            if let Some(start) = start_hint
                && types::user_key(&tf.meta.largest_key) < start
            {
                continue;
            }
            if let Some(end) = end_hint
                && types::user_key(&tf.meta.smallest_key) > end
            {
                continue;
            }
            if tf.meta.has_range_deletions {
                any_range_deletions = true;
            }
            let iter = TableIterator::new(tf.reader.clone());
            sources.push(IterSource::from_seekable(Box::new(iter)));
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
            let level_iter = LevelIterator::new(files.to_vec())
                .with_range_hints(start_hint.map(|s| s.to_vec()), end_hint.map(|e| e.to_vec()));
            sources.push(IterSource::from_seekable(Box::new(level_iter)));
        }

        let mut db_iter = DBIterator::from_sources(sources, seq);
        if let Some(end) = end_hint {
            db_iter.set_upper_bound(end.to_vec());
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
                if tf.meta.has_range_deletions
                    && let Ok(ts) = tf.reader.get_range_tombstones()
                {
                    for (b, e, s) in ts {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
            for level in 1..version.num_levels {
                for tf in version.level_files(level) {
                    if tf.meta.has_range_deletions
                        && let Ok(ts) = tf.reader.get_range_tombstones()
                    {
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

    /// Create a prefix-bounded iterator.
    ///
    /// Uses prefix bloom filters to skip SST files that don't contain the prefix,
    /// and stops iteration as soon as the prefix boundary is crossed.
    pub fn iter_with_prefix(&self, prefix: &[u8]) -> Result<DBIterator> {
        self.iter_with_prefix_seq(prefix, self.current_sequence())
    }

    /// Create a prefix-bounded iterator at a specific sequence number.
    fn iter_with_prefix_seq(&self, prefix: &[u8], seq: SequenceNumber) -> Result<DBIterator> {
        self.check_usable().c(d!())?;

        // Lock-free read via SuperVersion.
        let sv = self.get_super_version();
        let (active_mem, imm_mems, version) =
            (&sv.active_memtable, &sv.immutable_memtables, &sv.version);

        let mut sources: Vec<IterSource> = Vec::new();
        let mut any_range_deletions = false;

        // Active memtable
        {
            if active_mem.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(active_mem.clone());
            sources.push(IterSource::from_seekable(Box::new(cursor)));
        }

        // Immutable memtables
        for imm in imm_mems {
            if imm.has_range_deletions() {
                any_range_deletions = true;
            }
            let cursor = MemTableCursorIter::new(Arc::clone(imm));
            sources.push(IterSource::from_seekable(Box::new(cursor)));
        }

        // Compute prefix upper bound for range pruning
        let mut prefix_upper = prefix.to_vec();
        // Increment prefix to get exclusive upper bound
        let has_upper = {
            let mut carry = true;
            for byte in prefix_upper.iter_mut().rev() {
                if carry {
                    if *byte == 0xFF {
                        *byte = 0x00;
                    } else {
                        *byte += 1;
                        carry = false;
                    }
                }
            }
            !carry // false if prefix was all 0xFF (no upper bound)
        };

        // SST files — skip files via range pruning + prefix bloom.
        // L0: per-file filtering (files may overlap).
        for tf in version.level_files(0) {
            if types::user_key(&tf.meta.largest_key) < prefix {
                continue;
            }
            if has_upper && types::user_key(&tf.meta.smallest_key) >= prefix_upper.as_slice() {
                continue;
            }
            if !tf.reader.prefix_may_match(prefix) {
                continue;
            }
            if tf.meta.has_range_deletions {
                any_range_deletions = true;
            }
            let iter = TableIterator::new(tf.reader.clone());
            sources.push(IterSource::from_seekable(Box::new(iter)));
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
                .with_prefix(prefix.to_vec())
                .with_range_hints(
                    Some(prefix.to_vec()),
                    if has_upper {
                        Some(prefix_upper.clone())
                    } else {
                        None
                    },
                );
            sources.push(IterSource::from_seekable(Box::new(level_iter)));
        }

        let mut iter = DBIterator::from_sources_with_prefix(sources, seq, prefix.to_vec());

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
                if tf.meta.has_range_deletions
                    && let Ok(ts) = tf.reader.get_range_tombstones()
                {
                    for (b, e, s) in ts {
                        all_tombstones.push((b, e, s, 0));
                    }
                }
            }
            for level in 1..version.num_levels {
                for tf in version.level_files(level) {
                    if tf.meta.has_range_deletions
                        && let Ok(ts) = tf.reader.get_range_tombstones()
                    {
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

        // Seek to the prefix start
        iter.seek(prefix);

        Ok(iter)
    }

    /// Create a bidirectional iterator over all visible entries.
    ///
    /// Uses lazy streaming: forward iteration (`next()`) is streamed without
    /// materializing all entries. On first backward access (`next_back()`),
    /// remaining entries are collected into memory.
    pub fn iter_bidi(&self) -> Result<BidiIterator> {
        let db_iter = self.iter().c(d!())?;
        Ok(BidiIterator::lazy(db_iter))
    }

    /// Create a prefix iterator with lazy streaming.
    ///
    /// Forward iteration is streamed; backward access materializes on first `next_back()`.
    pub fn prefix_iterator(&self, prefix: &[u8]) -> Result<BidiIterator> {
        let db_iter = self.iter_with_prefix(prefix).c(d!())?;
        Ok(BidiIterator::lazy(db_iter))
    }

    /// Create a prefix iterator with options (supports snapshot for historical reads).
    pub fn prefix_iterator_with_options(
        &self,
        prefix: &[u8],
        options: &ReadOptions,
    ) -> Result<BidiIterator> {
        let seq = options.snapshot.unwrap_or_else(|| self.current_sequence());
        let db_iter = self.iter_with_prefix_seq(prefix, seq).c(d!())?;
        Ok(BidiIterator::lazy(db_iter))
    }

    pub fn snapshot_seq(&self) -> SequenceNumber {
        self.current_sequence()
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
        self.freeze_and_flush(&mut inner).c(d!())?;
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
                self.freeze_and_flush(&mut inner).c(d!())?;
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
                        &[],
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

    /// Get the current SuperVersion snapshot without locking `inner`.
    /// Readers call this instead of locking `inner` for iterator/get operations.
    fn get_super_version(&self) -> Arc<SuperVersion> {
        self.super_version.read().clone()
    }

    /// Refresh the SuperVersion from current inner state.
    /// Called after memtable freeze, flush, or compaction.
    /// Must be called while `inner` is locked (caller holds the lock).
    fn install_super_version(&self, inner: &DBInner) {
        let sv = Arc::new(SuperVersion {
            active_memtable: inner.active_memtable.clone(),
            immutable_memtables: inner.immutable_memtables.clone(),
            version: inner.versions.current(),
        });
        *self.super_version.write() = sv;
    }

    fn check_usable(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(eg!(Error::DbClosed));
        }
        if let Some(ref msg) = *self.bg_error.lock() {
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
            // Mark succeeded batches as Ok, failed as Err
            for &rp in &batch_group[..fail_idx] {
                let rr = unsafe { &mut *rp };
                rr.result = Some(Ok(()));
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
            *self.bg_error.lock() = Some(format!("WAL sync failed: {}", e));
            for &rp in batch_group {
                let rr = unsafe { &mut *rp };
                rr.result = Some(Err(eg!(Error::Io(std::io::Error::other(format!("{}", e))))));
            }
            return Err(eg!(Error::Io(std::io::Error::other(format!("{}", e)))));
        }

        // Check memtable size threshold
        let mut did_flush = false;
        if inner.active_memtable.approximate_size() >= self.options.write_buffer_size {
            match self.freeze_and_flush(&mut inner) {
                Ok(()) => did_flush = true,
                Err(e) => tracing::error!("auto-flush failed (data safe in WAL): {}", e),
            }
        }

        drop(inner);

        if did_flush {
            self.signal_compaction();
        }

        Ok(())
    }

    fn freeze_and_flush(&self, inner: &mut DBInner) -> Result<()> {
        let old_mem = std::mem::replace(&mut inner.active_memtable, Arc::new(MemTable::new()));

        // New WAL
        let new_wal_number = inner.versions.new_file_number();
        let new_wal_path = self.path.join(format!("{:06}.wal", new_wal_number));
        let new_wal = WalWriter::new(&new_wal_path).c(d!())?;
        let old_wal_number = inner.wal_number;
        inner.wal_writer = Some(new_wal);
        inner.wal_number = new_wal_number;

        // Allocate SST file number
        let sst_number = inner.versions.new_file_number();
        let sst_path = self.path.join(format!("{:06}.sst", sst_number));

        // Flush memtable → SST
        let build_result = self.write_memtable_to_sst(&old_mem, &sst_path).c(d!())?;

        // Record in MANIFEST
        let mut edit = VersionEdit::new();
        edit.set_log_number(new_wal_number);
        edit.set_next_file_number(inner.versions.next_file_number());
        edit.set_last_sequence(self.current_sequence());
        edit.add_file(
            0, // L0
            FileMetaData {
                number: sst_number,
                file_size: build_result.file_size,
                smallest_key: build_result.smallest_key.unwrap_or_default(),
                largest_key: build_result.largest_key.unwrap_or_default(),
                has_range_deletions: build_result.has_range_deletions,
            },
        );
        inner.versions.log_and_apply(edit).c(d!())?;
        self.stats.record_flush();

        // Pin new L0 file's index in cache for fast iterator creation
        if self.options.pin_l0_filter_and_index_blocks_in_cache {
            let version = inner.versions.current();
            for tf in version.level_files(0) {
                if tf.meta.number == sst_number {
                    tf.reader.pin_metadata_in_cache();
                    break;
                }
            }
        }

        // Update cached L0 count after flush
        self.l0_file_count
            .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
        self.install_super_version(inner);

        // Clean up old WAL
        let old_wal_path = self.path.join(format!("{:06}.wal", old_wal_number));
        let _ = std::fs::remove_file(old_wal_path);

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
                        &[],
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

    fn replay_wal_record(data: &[u8], mem: &MemTable, max_sequence: &mut u64) {
        if data.len() < 12 {
            return;
        }
        let seq = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let count = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let mut offset = 12;

        for i in 0..count {
            if offset >= data.len() {
                break;
            }
            let entry_seq = seq + i as u64;
            *max_sequence = (*max_sequence).max(entry_seq);

            let vt = data[offset];
            offset += 1;
            if offset + 4 > data.len() {
                break;
            }
            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + key_len > data.len() {
                break;
            }
            let key = &data[offset..offset + key_len];
            offset += key_len;

            match ValueType::from_u8(vt) {
                Some(ValueType::Value) => {
                    if offset + 4 > data.len() {
                        break;
                    }
                    let val_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + val_len > data.len() {
                        break;
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
                        break;
                    }
                    let val_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + val_len > data.len() {
                        break;
                    }
                    let value = &data[offset..offset + val_len];
                    offset += val_len;
                    mem.put(key, value, entry_seq, ValueType::RangeDeletion);
                }
                None => {}
            }
        }
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
    pub fn simulate_crash(self) {
        self.closed.store(true, Ordering::Release);
        self.compaction_shutdown.store(true, Ordering::Release);
        self.compaction_notify.1.notify_all();
        for handle in self.compaction_handles.lock().drain(..) {
            let _ = handle.join();
        }
        std::mem::forget(self);
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::Acquire) {
            self.closed.store(true, Ordering::Release);
            if let Some(ref mut wal) = self.inner.lock().wal_writer {
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
        assert_eq!(iter.key(), b"key_05");

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
