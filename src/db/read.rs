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
            for file in files.iter().filter(|f| f.meta.has_range_deletions) {
                tomb_seq = tomb_seq.max(
                    file.reader
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
            if mem.has_range_deletions() {
                tombstones.extend(
                    mem.get_range_tombstones()
                        .into_iter()
                        .filter(|(b, e, s)| {
                            *s <= sequence && tombstone_overlaps_bounds(b, e, lower, upper)
                        })
                        .map(|(b, e, s)| (b, e, s, 0)),
                );
            }
        }

        let mut config = IterConfig::new(lower, upper, &options.block_property_filters);
        config.prefix = prefix.map(Arc::from);
        for level in 0..version.num_levels {
            let files = version.level_files(level);
            if files.is_empty() {
                continue;
            }
            for file in files {
                // A point Bloom miss or a last point below the lower limit
                // cannot rule out a range deletion covering the scan.
                if upper.is_some_and(|hi| types::user_key(&file.meta.smallest_key) >= hi) {
                    continue;
                }
                if file.meta.has_range_deletions {
                    tombstones.extend(
                        file.reader
                            .read_tombstones(lower, upper, sequence, options.fill_cache)
                            .ctx()?
                            .into_iter()
                            .map(|(b, e, s)| (b, e, s, level)),
                    );
                }
                if level == 0
                    && !lower.is_some_and(|lo| types::user_key(&file.meta.largest_key) < lo)
                    && prefix.is_none_or(|p| file.reader.prefix_may_match(p))
                {
                    let iter =
                        TableIterator::new(file.reader.clone()).with_fill_cache(options.fill_cache);
                    sources.push(IterSource::from_table_iter(iter).with_level(0));
                }
            }
            if level > 0 {
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
