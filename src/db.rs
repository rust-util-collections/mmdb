//! Core DB implementation with WAL, MemTable, SST, MANIFEST, and Iterator.

use std::collections::VecDeque;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::Sender,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::cache::block_cache::BlockCache;
use crate::cache::table_cache::TableCache;
use crate::compaction::LeveledCompaction;
use crate::error::{Error, Result};
use crate::iterator::BidiIterator;
use crate::iterator::db_iter::DBIterator;
use crate::iterator::merge::IterSource;
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::manifest::version_set::VersionSet;
use crate::memtable::MemTable;
use crate::options::{DbOptions, ReadOptions, WriteOptions};
use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
use crate::sst::table_reader::TableIterator;
use crate::types::{SequenceNumber, ValueType, WriteBatch};
use crate::wal::{WalReader, WalWriter};

/// A pending write request for group commit.
struct WriteRequest {
    batch: WriteBatch,
    sync: bool,
    result: Option<Result<()>>,
    done: bool,
}

/// The core database handle.
pub struct DB {
    path: PathBuf,
    options: DbOptions,
    inner: Arc<Mutex<DBInner>>,
    /// Global sequence number (next to assign).
    sequence: AtomicU64,
    /// Write queue and condvar for group commit.
    write_queue: Mutex<VecDeque<*mut WriteRequest>>,
    write_cv: Condvar,
    closed: AtomicBool,
    block_cache: Arc<BlockCache>,
    table_cache: Arc<TableCache>,
    /// Channel to signal the background compaction thread.
    compaction_tx: Mutex<Option<Sender<()>>>,
    /// Background compaction thread handle.
    compaction_handle: Mutex<Option<JoinHandle<()>>>,
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

impl DB {
    /// Open or create a database.
    pub fn open(options: DbOptions, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if options.create_if_missing {
            std::fs::create_dir_all(&path)?;
        } else if !path.exists() {
            return Err(Error::InvalidArgument(format!(
                "DB path does not exist: {}",
                path.display()
            )));
        }

        if options.error_if_exists {
            let current = path.join("CURRENT");
            if current.exists() {
                return Err(Error::InvalidArgument(format!(
                    "DB already exists: {}",
                    path.display()
                )));
            }
        }

        // Create caches
        let block_cache = Arc::new(BlockCache::new(options.block_cache_capacity));
        let table_cache = Arc::new(TableCache::new(
            &path,
            options.max_open_files,
            Some(block_cache.clone()),
        ));

        // Open or create VersionSet (handles MANIFEST)
        let mut versions =
            VersionSet::open_with_cache(&path, options.num_levels, Some(table_cache.clone()))?;
        let mut max_sequence = versions.last_sequence();

        // Recover from any WAL files not yet flushed
        let mut active_memtable = Arc::new(MemTable::new());
        let mut wal_numbers: Vec<u64> = Vec::new();
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
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
            let mut builder = TableBuilder::new(&sst_path, build_opts)?;
            for (key, value) in active_memtable.iter() {
                builder.add(&key, &value)?;
            }
            let build_result = builder.finish()?;

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
                },
            );
            versions.log_and_apply(edit)?;

            // Reset the memtable — data is now safely in SST
            active_memtable = Arc::new(MemTable::new());
        }

        // Create a fresh WAL
        let wal_number = versions.new_file_number();
        let wal_path = path.join(format!("{:06}.wal", wal_number));
        let wal_writer = WalWriter::new(&wal_path)?;

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
            versions.log_and_apply(edit)?;
        }

        let sequence_start = max_sequence + 1;

        let inner = Arc::new(Mutex::new(DBInner {
            active_memtable,
            immutable_memtables: Vec::new(),
            wal_writer: Some(wal_writer),
            wal_number,
            versions,
        }));

        // Spawn background compaction thread
        let (compaction_tx, compaction_rx) = std::sync::mpsc::channel::<()>();
        let bg_inner = Arc::clone(&inner);
        let bg_path = path.clone();
        let bg_options = options.clone();
        let bg_table_cache = table_cache.clone();
        let compaction_handle = thread::Builder::new()
            .name("mmdb-compaction".into())
            .spawn(move || {
                while compaction_rx.recv().is_ok() {
                    // Drain any additional pending signals
                    while compaction_rx.try_recv().is_ok() {}
                    // Run all pending compactions
                    loop {
                        let mut inner = bg_inner.lock();
                        let version = inner.versions.current();
                        match LeveledCompaction::pick_compaction(&version, &bg_options) {
                            Some(task) => {
                                let _ = LeveledCompaction::execute_compaction_with_cache(
                                    &task,
                                    &mut inner.versions,
                                    &bg_path,
                                    &bg_options,
                                    Some(&bg_table_cache),
                                );
                            }
                            None => break,
                        }
                    }
                }
            })
            .expect("failed to spawn compaction thread");

        let db = Self {
            path,
            options,
            inner,
            sequence: AtomicU64::new(sequence_start),
            write_queue: Mutex::new(VecDeque::new()),
            write_cv: Condvar::new(),
            closed: AtomicBool::new(false),
            block_cache,
            table_cache,
            compaction_tx: Mutex::new(Some(compaction_tx)),
            compaction_handle: Mutex::new(Some(compaction_handle)),
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
        self.check_closed()?;
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_batch_inner(batch, write_options)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.check_closed()?;
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_batch_inner(batch, &WriteOptions::default())
    }

    /// Delete all keys in the range [begin, end).
    pub fn delete_range(&self, begin: &[u8], end: &[u8]) -> Result<()> {
        self.check_closed()?;
        let mut batch = WriteBatch::new();
        batch.delete_range(begin, end);
        self.write_batch_inner(batch, &WriteOptions::default())
    }

    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.check_closed()?;
        if batch.is_empty() {
            return Ok(());
        }
        self.write_batch_inner(batch, &WriteOptions::default())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_with_options(&ReadOptions::default(), key)
    }

    pub fn get_with_options(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_closed()?;

        let seq = match options.snapshot {
            Some(s) => s,
            None => self.current_sequence(),
        };

        // Snapshot current state
        let (active_mem, imm_mems, version) = {
            let inner = self.inner.lock();
            (
                inner.active_memtable.clone(),
                inner.immutable_memtables.clone(),
                inner.versions.current(),
            )
        };

        // Check range tombstones in memtables that may cover this key.
        // A range deletion InternalKey(begin, seq, RangeDeletion) → end
        // covers key if begin <= key < end and the tombstone seq <= our read seq.
        if self.is_key_range_deleted(key, seq, &active_mem, &imm_mems) {
            return Ok(None);
        }

        // 1. Active MemTable
        if let Some(result) = active_mem.get(key, seq) {
            return Ok(result);
        }

        // 2. Immutable MemTables (newest first)
        for imm in &imm_mems {
            if let Some(result) = imm.get(key, seq) {
                return Ok(result);
            }
        }

        // 3. L0 SST files (newest first, may overlap)
        for tf in version.level_files(0) {
            if let Some(result) = tf.reader.get_internal(key, seq)? {
                return Ok(result);
            }
        }

        // 4. L1+ SST files (sorted by smallest_key, no overlap within level)
        for level in 1..version.num_levels {
            let files = version.level_files(level);
            if files.is_empty() {
                continue;
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
                && let Some(result) = tf.reader.get_internal(key, seq)?
            {
                return Ok(result);
            }
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
        self.check_closed()?;

        let seq = match options.snapshot {
            Some(s) => s,
            None => self.current_sequence(),
        };

        let (active_mem, imm_mems, version) = {
            let inner = self.inner.lock();
            (
                inner.active_memtable.clone(),
                inner.immutable_memtables.clone(),
                inner.versions.current(),
            )
        };

        let mut sources: Vec<IterSource> = Vec::new();

        // Active memtable — cursor-based streaming iterator.
        {
            use crate::memtable::skiplist::MemTableCursorIter;
            let cursor = MemTableCursorIter::new(active_mem.clone());
            sources.push(IterSource::from_seekable(Box::new(cursor)));
        }

        // Immutable memtables
        for imm in &imm_mems {
            use crate::memtable::skiplist::MemTableCursorIter;
            let cursor = MemTableCursorIter::new(imm.clone());
            sources.push(IterSource::from_seekable(Box::new(cursor)));
        }

        // SST files — only include files whose key range overlaps [start_hint, end_hint].
        // L0 files can overlap each other so we check each individually.
        // L1+ files are sorted and non-overlapping, so we can binary-search.
        for level in 0..version.num_levels {
            for tf in version.level_files(level) {
                // Check if this file's key range overlaps the hint range.
                if let Some(start) = start_hint {
                    // File's largest user key must be >= start
                    let lk = &tf.meta.largest_key;
                    let file_largest_uk = if lk.len() >= 8 {
                        &lk[..lk.len() - 8]
                    } else {
                        lk.as_slice()
                    };
                    if file_largest_uk < start {
                        continue; // File ends before our range starts
                    }
                }
                if let Some(end) = end_hint {
                    // File's smallest user key must be <= end
                    let sk = &tf.meta.smallest_key;
                    let file_smallest_uk = if sk.len() >= 8 {
                        &sk[..sk.len() - 8]
                    } else {
                        sk.as_slice()
                    };
                    if file_smallest_uk > end {
                        continue; // File starts after our range ends
                    }
                }
                let iter = TableIterator::new(tf.reader.clone());
                sources.push(IterSource::from_seekable(Box::new(iter)));
            }
        }

        Ok(DBIterator::from_sources(sources, seq))
    }

    /// Create a prefix-bounded iterator.
    ///
    /// Uses prefix bloom filters to skip SST files that don't contain the prefix,
    /// and stops iteration as soon as the prefix boundary is crossed.
    pub fn iter_with_prefix(&self, prefix: &[u8]) -> Result<DBIterator> {
        self.check_closed()?;

        let seq = self.current_sequence();

        let (active_mem, imm_mems, version) = {
            let inner = self.inner.lock();
            (
                inner.active_memtable.clone(),
                inner.immutable_memtables.clone(),
                inner.versions.current(),
            )
        };

        let mut sources: Vec<IterSource> = Vec::new();

        // Active memtable
        {
            use crate::memtable::skiplist::MemTableCursorIter;
            let cursor = MemTableCursorIter::new(active_mem.clone());
            sources.push(IterSource::from_seekable(Box::new(cursor)));
        }

        // Immutable memtables
        for imm in &imm_mems {
            use crate::memtable::skiplist::MemTableCursorIter;
            let cursor = MemTableCursorIter::new(imm.clone());
            sources.push(IterSource::from_seekable(Box::new(cursor)));
        }

        // SST files — skip files where prefix bloom says prefix is absent
        for level in 0..version.num_levels {
            for tf in version.level_files(level) {
                // Check prefix bloom filter if available
                if !tf.reader.prefix_may_match(prefix) {
                    continue; // bloom says prefix not in this SST
                }
                let iter = TableIterator::new(tf.reader.clone());
                sources.push(IterSource::from_seekable(Box::new(iter)));
            }
        }

        let mut iter = DBIterator::from_sources_with_prefix(sources, seq, prefix.to_vec());
        // Seek to the prefix start
        iter.seek(prefix);
        Ok(iter)
    }

    /// Create a bidirectional iterator over all visible entries.
    ///
    /// Materializes entries at creation time. Supports `DoubleEndedIterator`
    /// for both forward and reverse traversal.
    pub fn iter_bidi(&self) -> Result<BidiIterator> {
        let entries: Vec<_> = self.iter()?.collect();
        Ok(BidiIterator::new(entries))
    }

    /// Create a bidirectional iterator over entries whose keys start with `prefix`.
    pub fn prefix_iterator(&self, prefix: &[u8]) -> Result<BidiIterator> {
        let prefix = prefix.to_vec();
        let entries: Vec<_> = self
            .iter()?
            .filter(|(k, _)| k.starts_with(&prefix))
            .collect();
        Ok(BidiIterator::new(entries))
    }

    /// Create a bidirectional iterator over entries within the given key range.
    ///
    /// Supports `Included`, `Excluded`, and `Unbounded` bounds.
    pub fn range<R: RangeBounds<Vec<u8>>>(&self, bounds: R) -> Result<BidiIterator> {
        let entries: Vec<_> = self.iter()?.filter(|(k, _)| bounds.contains(k)).collect();
        Ok(BidiIterator::new(entries))
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
    /// - "num-files-at-level{N}" — number of SST files at level N
    /// - "estimate-num-keys" — estimated number of keys
    /// - "total-sst-size" — total size of all SST files in bytes
    /// - "block-cache-usage" — approximate block cache entry count
    /// - "compaction-pending" — "1" if compaction is needed, "0" otherwise
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
            _ => None,
        }
    }

    /// Force flush the active MemTable to SST.
    pub fn flush(&self) -> Result<()> {
        self.check_closed()?;
        let _wg = self.write_queue.lock(); // serialize with writers
        let mut inner = self.inner.lock();
        if inner.active_memtable.is_empty() {
            return Ok(());
        }
        self.freeze_and_flush(&mut inner)?;
        self.maybe_compact(&mut inner)?;
        Ok(())
    }

    /// Run compaction if needed (L0 → L1 or Ln → Ln+1).
    /// Runs inline while holding locks for deterministic behavior.
    pub fn compact(&self) -> Result<()> {
        self.check_closed()?;
        let _wg = self.write_queue.lock();
        let mut inner = self.inner.lock();
        self.do_compaction(&mut inner)
    }

    /// Compact all keys in the given range across all levels.
    /// If `begin` is None, starts from the beginning. If `end` is None, goes to the end.
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.check_closed()?;

        // First flush memtable to ensure all data is in SSTs
        {
            let _wg = self.write_queue.lock();
            let mut inner = self.inner.lock();
            if !inner.active_memtable.is_empty() {
                self.freeze_and_flush(&mut inner)?;
            }
        }

        // Then compact all levels
        let _wg = self.write_queue.lock();
        let mut inner = self.inner.lock();
        let _ = (begin, end); // Range filtering is implicit through leveled compaction
        self.do_compaction(&mut inner)
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

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    // -- Internal --

    /// Signal background compaction thread (non-blocking).
    fn signal_compaction(&self) {
        if let Some(ref tx) = *self.compaction_tx.lock() {
            let _ = tx.send(());
        }
    }

    /// Check if a key is covered by any range tombstone visible at the given sequence.
    /// Only scans memtables that actually contain RangeDeletion entries (O(1) skip
    /// for the common case where delete_range() was never called).
    fn is_key_range_deleted(
        &self,
        key: &[u8],
        seq: SequenceNumber,
        active_mem: &MemTable,
        imm_mems: &[Arc<MemTable>],
    ) -> bool {
        use crate::types::InternalKeyRef;

        let check_memtable = |mem: &MemTable| -> bool {
            if !mem.has_range_deletions() {
                return false;
            }
            for (ikey, value) in mem.iter() {
                if ikey.len() < 8 {
                    continue;
                }
                let ikr = InternalKeyRef::new(&ikey);
                if ikr.value_type() != ValueType::RangeDeletion {
                    continue;
                }
                if ikr.sequence() > seq {
                    continue;
                }
                let begin = ikr.user_key();
                let end = &value;
                if key >= begin && key < end.as_slice() {
                    return true;
                }
            }
            false
        };

        if check_memtable(active_mem) {
            return true;
        }
        for imm in imm_mems {
            if check_memtable(imm) {
                return true;
            }
        }
        false
    }

    fn check_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::DbClosed);
        }
        Ok(())
    }

    fn current_sequence(&self) -> SequenceNumber {
        self.sequence.load(Ordering::Acquire).saturating_sub(1)
    }

    /// Apply write backpressure based on L0 file count.
    fn maybe_throttle_writes(&self) {
        let l0_count = {
            let inner = self.inner.lock();
            inner.versions.current().l0_file_count()
        };

        if l0_count >= self.options.l0_stop_trigger {
            // Block: try inline compaction to reduce L0 count
            let mut inner = self.inner.lock();
            while inner.versions.current().l0_file_count() >= self.options.l0_stop_trigger {
                let _ = self.do_compaction(&mut inner);
                if inner.versions.current().l0_file_count() >= self.options.l0_stop_trigger {
                    break; // Can't compact further
                }
            }
        } else if l0_count >= self.options.l0_slowdown_trigger {
            // Progressive delay: more L0 files → longer sleep
            let delay_us = (l0_count - self.options.l0_slowdown_trigger + 1) as u64 * 1000;
            thread::sleep(Duration::from_micros(delay_us));
        }
    }

    fn write_batch_inner(&self, batch: WriteBatch, write_options: &WriteOptions) -> Result<()> {
        if write_options.no_slowdown {
            let l0_count = {
                let inner = self.inner.lock();
                inner.versions.current().l0_file_count()
            };
            if l0_count >= self.options.l0_slowdown_trigger {
                return Err(Error::InvalidArgument(
                    "write stalled: no_slowdown is set".to_string(),
                ));
            }
        } else {
            self.maybe_throttle_writes();
        }

        let mut req = WriteRequest {
            batch,
            sync: write_options.sync,
            result: None,
            done: false,
        };
        let req_ptr: *mut WriteRequest = &mut req;

        // Enqueue and check if we're the leader
        let mut queue = self.write_queue.lock();
        queue.push_back(req_ptr);
        let is_leader = queue.len() == 1;

        if !is_leader {
            // Wait for the leader to process our request
            self.write_cv.wait_while(&mut queue, |_| !req.done);
            return req.result.take().unwrap_or(Ok(()));
        }

        // Leader: collect all pending writes from the queue
        let batch_group: Vec<*mut WriteRequest> = queue.drain(..).collect();
        drop(queue); // release queue lock while doing I/O

        // Allocate sequences and write WAL for all batches
        let mut need_sync = false;
        let mut inner = self.inner.lock();

        // Check if any request has disable_wal set
        let disable_wal = write_options.disable_wal;

        for &req_ptr in &batch_group {
            // SAFETY: we hold the only references; other threads are blocked on write_cv
            let r = unsafe { &mut *req_ptr };
            let first_seq = self
                .sequence
                .fetch_add(r.batch.len() as u64, Ordering::AcqRel);
            need_sync |= r.sync;

            if !disable_wal {
                let wal_record = Self::encode_wal_record(first_seq, &r.batch);
                if let Some(ref mut wal) = inner.wal_writer
                    && let Err(e) = wal.add_record(&wal_record)
                {
                    // Mark all remaining as failed
                    for &rp in &batch_group {
                        let rr = unsafe { &mut *rp };
                        rr.result = Some(Err(Error::Io(std::io::Error::other(format!("{}", e)))));
                        rr.done = true;
                    }
                    self.write_cv.notify_all();
                    return Err(e);
                }
            }

            // Apply to memtable
            for (i, entry) in r.batch.entries.iter().enumerate() {
                let seq = first_seq + i as u64;
                inner.active_memtable.put(
                    &entry.key,
                    entry.value.as_deref().unwrap_or(&[]),
                    seq,
                    entry.value_type,
                );
            }
        }

        // Single fsync for all batches (skip when WAL is disabled)
        if !disable_wal && let Some(ref mut wal) = inner.wal_writer {
            let wal_result = if need_sync { wal.sync() } else { wal.flush() };
            if let Err(e) = wal_result {
                // WAL sync/flush failed — data may not be durable.
                // Notify all followers of the failure.
                let queue = self.write_queue.lock();
                for &req_ptr in &batch_group[1..] {
                    let r = unsafe { &mut *req_ptr };
                    r.result = Some(Err(Error::Io(std::io::Error::other(format!("{}", e)))));
                    r.done = true;
                }
                self.write_cv.notify_all();
                drop(queue);
                return Err(e);
            }
        }

        // Check memtable size threshold — data is already in WAL so a flush
        // failure here is not fatal, just log it.
        let mut did_flush = false;
        if inner.active_memtable.approximate_size() >= self.options.write_buffer_size {
            match self.freeze_and_flush(&mut inner) {
                Ok(()) => did_flush = true,
                Err(e) => tracing::error!("auto-flush failed (data safe in WAL): {}", e),
            }
        }

        drop(inner);

        // Signal background compaction after successful flush
        if did_flush {
            self.signal_compaction();
        }

        // Notify all followers
        let queue = self.write_queue.lock();
        for &req_ptr in &batch_group[1..] {
            let r = unsafe { &mut *req_ptr };
            if r.result.is_none() {
                r.result = Some(Ok(()));
            }
            r.done = true;
        }
        self.write_cv.notify_all();
        drop(queue);

        Ok(())
    }

    fn freeze_and_flush(&self, inner: &mut DBInner) -> Result<()> {
        let old_mem = std::mem::replace(&mut inner.active_memtable, Arc::new(MemTable::new()));

        // New WAL
        let new_wal_number = inner.versions.new_file_number();
        let new_wal_path = self.path.join(format!("{:06}.wal", new_wal_number));
        let new_wal = WalWriter::new(&new_wal_path)?;
        let old_wal_number = inner.wal_number;
        inner.wal_writer = Some(new_wal);
        inner.wal_number = new_wal_number;

        // Allocate SST file number
        let sst_number = inner.versions.new_file_number();
        let sst_path = self.path.join(format!("{:06}.sst", sst_number));

        // Flush memtable → SST
        let build_result = self.write_memtable_to_sst(&old_mem, &sst_path)?;

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
            },
        );
        inner.versions.log_and_apply(edit)?;

        // Clean up old WAL
        let old_wal_path = self.path.join(format!("{:06}.wal", old_wal_number));
        let _ = std::fs::remove_file(old_wal_path);

        Ok(())
    }

    fn maybe_compact(&self, inner: &mut DBInner) -> Result<()> {
        let version = inner.versions.current();
        if version.l0_file_count() >= self.options.l0_compaction_trigger {
            self.do_compaction(inner)?;
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
                    LeveledCompaction::execute_compaction_with_cache(
                        &task,
                        &mut inner.versions,
                        &self.path,
                        &self.options,
                        Some(&self.table_cache),
                    )?;
                }
                None => break,
            }
        }
        // Force-merge levels that have multiple files (space reclamation).
        // With background compaction, tombstones may end up in separate files
        // from the data they cover. Merging fixes this.
        for level in 1..self.options.num_levels {
            let is_bottommost = level >= self.options.num_levels - 1;
            LeveledCompaction::force_merge_level(
                level,
                is_bottommost,
                &mut inner.versions,
                &self.path,
                &self.options,
                Some(&self.table_cache),
            )?;
        }
        Ok(())
    }

    fn encode_wal_record(sequence: u64, batch: &WriteBatch) -> Vec<u8> {
        let mut buf = Vec::new();
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

        let mut builder = TableBuilder::new(path, opts)?;
        for (key, value) in mem.iter() {
            builder.add(&key, &value)?;
        }
        builder.finish()
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
        // Shut down background compaction thread: drop sender to close channel
        // (causes recv() to return Err), then join the thread.
        drop(self.compaction_tx.lock().take());
        if let Some(handle) = self.compaction_handle.lock().take() {
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
