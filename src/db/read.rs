//! Read resources shared by the DB handle and in-flight snapshot reads.
//! Snapshot registrations retain only a weak reference to this state.
use super::*;

pub(super) struct ReadState {
    /// Whether this handle was opened without write capability.
    pub(super) read_only: bool,
    /// Last sequence number committed and visible to readers/snapshots.
    pub(super) committed_sequence: Arc<AtomicU64>,
    pub(super) closed: AtomicBool,
    pub(super) db_id: u64,
    /// Fast-check flag for background errors (avoids Mutex lock on every read).
    pub(super) has_bg_error: Arc<AtomicBool>,
    /// Background error (e.g. WAL sync failure). Once set, all subsequent
    /// operations are rejected. Models RocksDB's background error state.
    pub(super) bg_error: Arc<Mutex<Option<String>>>,
    /// Shared view of `VersionSet::poisoned` (see `poison_flag`). Lets
    /// `check_usable` fail fast on a poisoned MANIFEST without locking
    /// `inner`, and lets sync sites that go through `manifest_sync_handle`
    /// poison the writer when durability cannot be confirmed.
    pub(super) manifest_poisoned: Arc<AtomicBool>,
    pub(super) stats: Arc<DbStats>,
    /// Notification condvar for compaction threads: (has_work, condvar).
    pub(super) compaction_notify: Arc<(Mutex<bool>, Condvar)>,
    /// Lock-free read snapshot (SuperVersion).
    /// Readers do a single atomic load. Writers swap atomically after
    /// memtable/version changes — no RwLock contention.
    pub(super) super_version: Arc<ArcSwap<SuperVersion>>,
    /// Read-triggered compaction hints accumulated from the get path, with
    /// at most one pending hint per sampled level while workers are busy.
    pub(super) read_compaction_hints: Arc<Mutex<Vec<CompactionHint>>>,
    /// Counter for periodic read-compaction checks.
    pub(super) read_counter: AtomicU64,
}

impl ReadState {
    pub(super) fn point_lookup<V: crate::pinned::PointValue>(
        &self,
        options: &ReadOptions<'_>,
        key: &[u8],
    ) -> Result<Option<V>> {
        // Pin storage before choosing the committed read sequence.
        let sv = self.get_super_version();
        self.check_usable().ctx()?;
        let sequence = self.resolve_read_sequence(options.snapshot)?;
        let mut tomb_seq = self.max_covering_tombstone_seq(
            key,
            sequence,
            &sv.active_memtable,
            &sv.immutable_memtables,
        );

        let visible = |value: Option<V>, entry_seq, covering_seq| {
            if covering_seq > entry_seq {
                return None;
            }
            if let Some(v) = &value {
                self.stats.record_read((key.len() + v.byte_len()) as u64);
            }
            value
        };
        for mem in std::iter::once(&sv.active_memtable).chain(&sv.immutable_memtables) {
            if let Some((value, seq)) = mem.point_value(key, sequence) {
                return Ok(visible(value, seq, tomb_seq));
            }
        }
        if tomb_seq > 0 {
            return Ok(None);
        }

        let version = &sv.version;
        for level in 0..version.num_levels {
            let files = version.level_files(level);
            // Tombstone ends may extend beyond the last point in an SST.
            // Check covering deletions before resolving any point at this level.
            for reader in version.range_readers(level) {
                if !reader
                    .range_deletion_bounds()
                    .is_some_and(|(begin, end)| begin <= key && key < end)
                {
                    continue;
                }
                #[cfg(test)]
                tests::RANGE_DIRECTORY_SEARCHES.with(|n| n.set(n.get() + 1));
                tomb_seq = tomb_seq.max(
                    reader
                        .covering_sequence(key, sequence, options.fill_cache)
                        .ctx()?,
                );
            }
            let candidates = if level == 0 {
                files
            } else {
                // L1+ has disjoint point-key ranges ordered by smallest key.
                let end = files.partition_point(|f| types::user_key(&f.meta.smallest_key) <= key);
                &files[end.saturating_sub(1)..end]
            };
            for file in candidates {
                if key < types::user_key(&file.meta.smallest_key)
                    || key > types::user_key(&file.meta.largest_key)
                {
                    continue;
                }
                if let Some((value, seq)) = file
                    .reader
                    .point_value(key, sequence, options.fill_cache)
                    .ctx()?
                {
                    if level >= 2 {
                        self.stats.maybe_sample_read_level(level);
                        self.maybe_check_read_compaction();
                    }
                    return Ok(visible(value, seq, tomb_seq));
                }
            }
        }
        Ok(None)
    }

    pub(super) fn scan(
        &self,
        options: &ReadOptions<'_>,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        prefix: Option<&[u8]>,
        batch: Option<&WriteBatchWithIndex>,
    ) -> Result<DBIterator> {
        // Retain the sources before choosing a visibility sequence. Compaction
        // can replace those sources and discard their old versions concurrently.
        let sv = self.get_super_version();
        self.check_usable().ctx()?;
        let sequence = self.resolve_read_sequence(options.snapshot)?;
        let (lower, upper) = intersect_iter_bounds(options, lower, upper);
        if lower.zip(upper).is_some_and(|(lo, hi)| lo >= hi) {
            return Ok(DBIterator::from_sources(Vec::new(), sequence));
        }

        let version = &sv.version;
        let mut sources = Vec::with_capacity(
            2 + sv.immutable_memtables.len() + version.level_files(0).len() + version.num_levels,
        );
        let mut tombstones = Vec::new();
        if let Some(batch) = batch {
            let base = MAX_SEQUENCE_NUMBER
                .checked_sub(batch.operation_count() - 1)
                .filter(|&base| base > sequence)
                .ok_or_else(|| Error::invalid_argument("sequence number space exhausted"))?;
            sources.push(IterSource::new(batch.sorted_entries(base).ctx()?).with_level(0));
            tombstones.extend(
                batch
                    .range_tombstones()
                    .map(|(lo, hi, pos)| (lo.to_vec(), hi.to_vec(), base + pos, 0)),
            );
        }
        for mem in std::iter::once(&sv.active_memtable).chain(&sv.immutable_memtables) {
            sources.push(
                IterSource::from_memtable(MemTableCursorIter::new(mem.clone())).with_level(0),
            );
            mem.with_range_tombstones(|ranges| {
                tombstones.extend(
                    ranges
                        .iter()
                        .filter(|rt| {
                            rt.seq <= sequence
                                && tombstone_overlaps_bounds(&rt.begin, &rt.end, lower, upper)
                        })
                        .map(|rt| (rt.begin.clone(), rt.end.clone(), rt.seq, 0)),
                );
            });
        }

        let mut config = IterConfig::new(lower, upper, &options.block_property_filters);
        config.prefix = prefix.map(Arc::from);
        for level in 0..version.num_levels {
            let files = version.level_files(level);
            if files.is_empty() {
                continue;
            }
            for reader in version.range_readers(level) {
                if (lower.is_some() || upper.is_some())
                    && !reader.range_deletion_bounds().is_some_and(|(begin, end)| {
                        tombstone_overlaps_bounds(begin, end, lower, upper)
                    })
                {
                    continue;
                }
                #[cfg(test)]
                tests::RANGE_DIRECTORY_SEARCHES.with(|n| n.set(n.get() + 1));
                tombstones.extend(
                    reader
                        .read_tombstones(lower, upper, sequence, options.fill_cache)
                        .ctx()?
                        .into_iter()
                        .map(|(b, e, s)| (b, e, s, level)),
                );
            }
            if level == 0 {
                for file in files {
                    if upper.is_some_and(|hi| types::user_key(&file.meta.smallest_key) >= hi)
                        || lower.is_some_and(|lo| types::user_key(&file.meta.largest_key) < lo)
                        || prefix.is_some_and(|p| !file.reader.prefix_may_match(p))
                    {
                        continue;
                    }
                    let iter =
                        TableIterator::new(file.reader.clone()).with_fill_cache(options.fill_cache);
                    sources.push(IterSource::from_table_iter(iter).with_level(0));
                }
            } else {
                let iter = LevelIterator::for_level(version.clone(), level)
                    .with_fill_cache(options.fill_cache);
                sources.push(IterSource::from_level_iter(iter).with_level(level));
            }
        }

        let mut iter = DBIterator::from_sources(sources, sequence);
        if batch.is_some() {
            iter.include_batch_source();
        }
        if !tombstones.is_empty() {
            iter.set_range_tombstones_with_levels(tombstones);
        }
        iter.set_config(Arc::new(config));
        if let Some(skip) = &options.skip_point {
            iter.set_skip_point(skip.clone());
        }
        iter.prime_lower_bound();
        Ok(iter)
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
    pub(super) fn get_super_version(&self) -> arc_swap::Guard<Arc<SuperVersion>> {
        #[cfg(test)]
        if let Some(hook) = BEFORE_READ_VIEW.with(|h| h.borrow_mut().take()) {
            hook();
        }
        self.super_version.load()
    }

    /// Signal background compaction threads (non-blocking).
    pub(super) fn signal_compaction(&self) {
        if self.read_only {
            return;
        }
        let (lock, cvar) = &*self.compaction_notify;
        let mut has_work = lock.lock();
        *has_work = true;
        cvar.notify_one();
    }

    pub(super) fn fail_stop_error(&self) -> Option<Error> {
        if self.has_bg_error.load(Ordering::Acquire)
            && let Some(ref msg) = *self.bg_error.lock()
        {
            return Some(Error::background(msg.clone()));
        }
        if self.manifest_poisoned.load(Ordering::Acquire) {
            return Some(Error::background(
                "MANIFEST writer poisoned by an earlier write failure; \
                 reopen the database to recover"
                    .to_string(),
            ));
        }
        None
    }

    /// Periodically check read-level samples and generate compaction hints.
    /// Called every 1024 reads from get_with_options.
    fn maybe_check_read_compaction(&self) {
        if self.read_only {
            return;
        }
        let count = self.read_counter.fetch_add(1, Ordering::Relaxed);
        if !count.is_multiple_of(1024) {
            return;
        }
        self.check_read_compaction();
    }

    /// Take read-level samples and convert them into compaction hints.
    pub(super) fn check_read_compaction(&self) {
        let samples = self.stats.take_read_level_samples();
        let mut hints = self.read_compaction_hints.lock();
        for (level, &count) in samples.iter().enumerate() {
            if count > 0 && level >= 2 && !hints.iter().any(|hint| hint.level == level) {
                hints.push(CompactionHint { level });
            }
        }
        if !hints.is_empty() {
            self.signal_compaction();
        }
    }

    pub(super) fn check_usable(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::db_closed());
        }
        if let Some(error) = self.fail_stop_error() {
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn current_sequence(&self) -> SequenceNumber {
        self.committed_sequence.load(Ordering::Acquire)
    }

    fn resolve_read_sequence(&self, snapshot: Option<SnapshotRef<'_>>) -> Result<SequenceNumber> {
        match snapshot {
            Some(snapshot) => snapshot.sequence_for(self),
            None => Ok(self.current_sequence()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    thread_local! {
        pub(super) static RANGE_DIRECTORY_SEARCHES: Cell<usize> = const { Cell::new(0) };
    }

    #[test]
    fn tombstone_discovery_visits_only_files_with_range_deletions() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions::default()
            .with_num_levels(2)
            .with_target_file_size_base(1);
        let db = DB::open(opts.clone(), dir.path()).unwrap();
        db.ingest_sorted((0..64u8).map(|i| ([i], [i]))).unwrap();
        assert_eq!(
            db.get_property("num-files-at-level1").as_deref(),
            Some("64")
        );

        let check = |db: &DB, deleted: bool| {
            // Boundary-adjacent windows never enter the unrelated table's range directory.
            for key in [29, 32, 40] {
                let covered = deleted && key == 32;
                let expected = (!covered).then_some(vec![key]);
                let visits = usize::from(covered);
                RANGE_DIRECTORY_SEARCHES.with(|n| n.set(0));
                assert_eq!(db.get(&[key, 0]).unwrap(), None);
                assert_eq!(
                    RANGE_DIRECTORY_SEARCHES.with(|n| n.replace(0)),
                    visits,
                    "point miss at {key}"
                );
                assert_eq!(db.get(&[key]).unwrap(), expected);
                assert_eq!(
                    RANGE_DIRECTORY_SEARCHES.with(|n| n.replace(0)),
                    visits,
                    "point lookup at {key}"
                );
                let mut iter = db
                    .iter_with_range(&ReadOptions::default(), Some(&[key]), Some(&[key + 1]))
                    .unwrap();
                assert_eq!(iter.step_next().unwrap().map(|(_, v)| v.to_vec()), expected);
                assert_eq!(
                    RANGE_DIRECTORY_SEARCHES.with(Cell::get),
                    visits,
                    "bounded scan at {key}"
                );
            }
            RANGE_DIRECTORY_SEARCHES.with(|n| n.set(0));
            let mut all = db.iter().unwrap();
            let mut count = 0;
            while all.step_next().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, if deleted { 54 } else { 64 });
            assert_eq!(
                RANGE_DIRECTORY_SEARCHES.with(Cell::get),
                usize::from(deleted)
            );
        };
        check(&db, false);
        // The tombstone starts below the scan and covers a point in a deeper SST.
        db.delete_range(&[30], &[40]).unwrap();
        db.flush().unwrap();
        check(&db, true);
        db.close().unwrap();
        let reopened = DB::open_read_only(dir.path()).unwrap();
        check(&reopened, true);
        reopened.close().unwrap();

        let db = DB::open(opts, dir.path()).unwrap();
        let old_version = db.inner.lock().versions.current();
        db.compact().unwrap();
        assert_eq!(old_version.range_readers(0).len(), 1);
        assert!(
            db.inner
                .lock()
                .versions
                .current()
                .range_readers(0)
                .is_empty()
        );
        assert_eq!(db.get(&[32]).unwrap(), None);
    }

    #[test]
    fn memtable_scan_copies_only_visible_overlapping_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        db.put(b"z", b"visible").unwrap();
        let snapshot = db.snapshot();
        for i in 0..256u16 {
            let mut begin = vec![b'a'; 1024];
            begin[1..3].copy_from_slice(&i.to_be_bytes());
            let mut end = begin.clone();
            end[1023] += 1;
            db.delete_range(&begin, &end).unwrap();
        }
        // First scan excludes every deletion by key. The snapshot scans the
        // full keyspace and excludes the same deletions by sequence.
        for (options, lower) in [
            (ReadOptions::default(), Some(&b"z"[..])),
            (snapshot.read_options(), None),
        ] {
            let (mut iter, bytes) = crate::test_alloc::count_allocated(|| {
                db.iter_with_range(&options, lower, None).unwrap()
            });
            assert!(bytes < 32 * 1024, "cursor allocated {bytes} bytes");
            assert_eq!(
                iter.step_next().unwrap(),
                Some((b"z".as_slice(), b"visible".as_slice()))
            );
            assert!(iter.step_next().unwrap().is_none());
        }
    }

    #[test]
    fn warm_point_reads_do_not_allocate_the_result_value() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        db.put(b"large", &vec![7; 1 << 20]).unwrap();
        for flushed in [false, true] {
            if flushed {
                db.flush().unwrap();
            }
            drop(db.get_pinned(b"large").unwrap().unwrap());
            let (pin, bytes) =
                crate::test_alloc::count_allocated(|| db.get_pinned(b"large").unwrap().unwrap());
            assert!(bytes < 1024, "point pin allocated {bytes} bytes");
            let (present, bytes) =
                crate::test_alloc::count_allocated(|| db.contains_key(b"large").unwrap());
            assert!(
                present && bytes < 1024,
                "contains_key allocated {bytes} bytes"
            );
            assert_eq!(pin.len(), 1 << 20);
        }
    }
}
