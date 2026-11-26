//! One worker installs immutable generations in WAL order. Progress is
//! published only after the corresponding MANIFEST edit is durable.
use super::*;

#[cfg(test)]
type FlushNotifyHook = Box<dyn FnOnce(&FlushContext)>;

#[cfg(test)]
thread_local! {
    static BEFORE_FLUSH_NOTIFY: std::cell::RefCell<Option<FlushNotifyHook>> =
        const { std::cell::RefCell::new(None) };
}

#[derive(Default)]
struct Progress {
    submitted: u64,
    durable: u64,
    pending: VecDeque<FrozenMemtable>,
    prepare_standby: bool,
    shutdown: bool,
    error: Option<Error>,
}

#[derive(Default)]
pub(super) struct FlushQueue {
    progress: Mutex<Progress>,
    changed: Condvar,
    standby_wal: Mutex<Option<(u64, WalWriter)>>,
    pub(super) standby_armed: AtomicBool,
    #[cfg(test)]
    before_build: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    on_fail: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl FlushQueue {
    #[cfg(test)]
    pub(super) fn on_next_build(&self, hook: impl FnOnce() + Send + 'static) {
        *self.before_build.lock() = Some(Box::new(hook));
    }

    #[cfg(test)]
    pub(super) fn on_next_fail(&self, hook: impl FnOnce() + Send + 'static) {
        *self.on_fail.lock() = Some(Box::new(hook));
    }

    pub(super) fn submit(&self, frozen: FrozenMemtable) -> u64 {
        let mut p = self.progress.lock();
        p.submitted += 1;
        p.pending.push_back(frozen);
        self.changed.notify_all();
        p.submitted
    }

    pub(super) fn target(&self) -> u64 {
        self.progress.lock().submitted
    }

    pub(super) fn fail(&self, error: Error) {
        self.progress.lock().error.get_or_insert(error);
        self.changed.notify_all();
        #[cfg(test)]
        if let Some(hook) = self.on_fail.lock().take() {
            hook();
        }
    }

    pub(super) fn check_capacity(&self, limit: usize, no_slowdown: bool) -> Result<()> {
        let mut p = self.progress.lock();
        loop {
            if let Some(e) = &p.error {
                return Err(e.clone());
            }
            if p.submitted - p.durable < limit as u64 {
                return Ok(());
            }
            if no_slowdown {
                return Err(Error::busy("immutable memtable queue is full"));
            }
            crate::profile::timed!(
                immutable_wait_ns,
                immutable_waits,
                self.changed.wait(&mut p)
            );
        }
    }

    pub(super) fn wait(&self, target: u64) -> Result<()> {
        let mut p = self.progress.lock();
        while p.durable < target {
            if let Some(e) = &p.error {
                return Err(e.clone());
            }
            self.changed.wait(&mut p);
        }
        Ok(())
    }

    pub(super) fn stop(&self) {
        let mut p = self.progress.lock();
        p.shutdown = true;
        p.prepare_standby = false;
        p.pending.clear();
        self.changed.notify_all();
    }

    #[cfg(test)]
    pub(super) fn has_standby(&self) -> bool {
        self.standby_wal.lock().is_some()
    }

    pub(super) fn request_standby(&self) {
        let mut p = self.progress.lock();
        if !p.shutdown && p.error.is_none() && !p.prepare_standby {
            p.prepare_standby = true;
            self.changed.notify_all();
        }
    }

    pub(super) fn take_standby_wal(&self) -> Option<(u64, WalWriter)> {
        let taken = self.standby_wal.lock().take();
        self.standby_armed.store(false, Ordering::Release);
        taken
    }

    pub(super) fn restore_standby_wal(&self, num: u64, writer: WalWriter) {
        let mut slot = self.standby_wal.lock();
        if slot.is_none() {
            *slot = Some((num, writer));
            self.standby_armed.store(true, Ordering::Release);
        }
    }

    pub(super) fn cleanup_standby_wal(&self, storage_path: &Path) {
        if let Some((num, writer)) = self.standby_wal.lock().take() {
            drop(writer);
            let _ = fs::remove_file(storage_path.join(format!("{num:06}.wal")));
        }
        self.standby_armed.store(false, Ordering::Release);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn abandon_standby_wal_for_crash(&self) {
        if let Some((_, writer)) = self.standby_wal.lock().take() {
            writer.discard_buffer();
        }
        self.standby_armed.store(false, Ordering::Release);
    }

    pub(super) fn run(&self, ctx: &FlushContext) {
        enum Work {
            Flush(FrozenMemtable),
            Standby,
        }
        loop {
            let work = {
                let mut p = self.progress.lock();
                loop {
                    if p.shutdown || p.error.is_some() {
                        return;
                    }
                    if let Some(frozen) = p.pending.pop_front() {
                        break Work::Flush(frozen);
                    }
                    if p.prepare_standby {
                        p.prepare_standby = false;
                        break Work::Standby;
                    }
                    self.changed.wait(&mut p);
                }
            };
            match work {
                Work::Standby => {
                    ctx.prepare_standby_wal(self);
                }
                Work::Flush(frozen) => {
                    #[cfg(test)]
                    if let Some(hook) = self.before_build.lock().take() {
                        hook();
                    }
                    let result = catch_unwind(AssertUnwindSafe(|| ctx.flush(&frozen)))
                        .unwrap_or_else(|_| Err(Error::background("flush worker panicked")));
                    if let Err(e) = result {
                        *ctx.bg_error.lock() = Some(e.to_string());
                        ctx.has_bg_error.store(true, Ordering::Release);
                        self.fail(e);
                        return;
                    }
                    self.progress.lock().durable += 1;
                    self.changed.notify_all();
                }
            }
        }
    }
}

pub(super) struct FlushContext {
    pub(super) inner: Arc<Mutex<DBInner>>,
    pub(super) storage_path: PathBuf,
    pub(super) dir_sync: Arc<Mutex<Option<fs::File>>>,
    pub(super) options: DbOptions,
    pub(super) builds: Arc<SstBuilds>,
    pub(super) stats: Arc<DbStats>,
    pub(super) claims: Arc<CompactionClaims>,
    pub(super) table_cache: Arc<TableCache>,
    pub(super) super_version: Arc<ArcSwap<SuperVersion>>,
    pub(super) l0_file_count: Arc<AtomicUsize>,
    pub(super) compaction_notify: Arc<(Mutex<bool>, Condvar)>,
    pub(super) manifest_poisoned: Arc<AtomicBool>,
    pub(super) has_bg_error: Arc<AtomicBool>,
    pub(super) bg_error: Arc<Mutex<Option<String>>>,
}

pub(super) fn file_metadata((number, result): (u64, TableBuildResult)) -> FileMetaData {
    FileMetaData {
        number,
        file_size: result.file_size,
        smallest_key: result.smallest_key.unwrap_or_default(),
        largest_key: result.largest_key.unwrap_or_default(),
        has_range_deletions: result.has_range_deletions,
    }
}

impl FlushContext {
    pub(super) fn prepare_standby_wal(&self, queue: &FlushQueue) {
        if queue.standby_wal.lock().is_some() {
            return;
        }
        let Ok(standby_num) = self.inner.lock().versions.file_numbers.allocate() else {
            queue.standby_armed.store(false, Ordering::Release);
            return;
        };
        let path = self.storage_path.join(format!("{standby_num:06}.wal"));
        let writer = match WalWriter::new(&path) {
            Ok(w) => w,
            Err(e) => {
                let _ = fs::remove_file(&path);
                queue.standby_armed.store(false, Ordering::Release);
                tracing::warn!(
                    "standby WAL {:06} pre-creation failed, deferring to rotation: {}",
                    standby_num,
                    e
                );
                return;
            }
        };
        let stopped = {
            let p = queue.progress.lock();
            p.shutdown || p.error.is_some()
        };
        if stopped {
            drop(writer);
            let _ = fs::remove_file(&path);
            return;
        }
        let inner = self.inner.lock();
        let mut slot = queue.standby_wal.lock();
        if slot.is_some() || standby_num <= inner.wal_number {
            drop(slot);
            drop(inner);
            drop(writer);
            let _ = fs::remove_file(&path);
            return;
        }
        *slot = Some((standby_num, writer));
    }

    pub(super) fn flush(&self, frozen: &FrozenMemtable) -> Result<()> {
        let _build = self.builds.enter();
        let outputs = DB::write_memtable_ssts(
            &frozen.old_mem,
            &self.storage_path,
            &|| l0_build_opts(&self.options),
            &frozen.file_numbers,
            &frozen.snapshots,
        )
        .ctx()?;
        let pending = PendingSsts::new(&self.storage_path, &frozen.file_numbers);
        pending.track(outputs.iter().map(|(n, _)| *n));
        self.table_cache.prewarm(outputs.iter().map(|(n, _)| *n));
        let pins: Vec<_> = outputs
            .iter()
            .filter_map(|(n, _)| {
                if !self.options.pin_l0_first_block_and_warm_index {
                    return None;
                }
                let reader = self.table_cache.get_reader(*n).ok()?;
                Some((*n, reader.prepare_first_block_pin()?))
            })
            .collect();
        let mut edit = VersionEdit::new();
        edit.set_log_number(frozen.new_wal_number);
        edit.set_last_sequence(frozen.last_sequence);
        for output in outputs {
            edit.add_file(0, file_metadata(output));
        }
        let synced = sync_new_sst_parent_dir(&self.dir_sync, &self.manifest_poisoned)?;
        let output_numbers: Vec<_> = edit.new_files.iter().map(|(_, m)| m.number).collect();
        let (claim, manifest) = {
            let mut inner = self.inner.lock();
            edit.set_next_file_number(inner.versions.next_file_number());
            inner.versions.log_and_apply_synced(edit, synced).ctx()?;
            pending.keep();
            let version = inner.versions.current();
            #[cfg(feature = "bench-metrics")]
            {
                let mut p = self.stats.l0_profile.lock();
                p.peak_installed_l0_files = p
                    .peak_installed_l0_files
                    .max(version.l0_file_count() as u64);
            }
            for (number, pin) in pins {
                if let Some(tf) = version
                    .level_files(0)
                    .iter()
                    .find(|tf| tf.meta.number == number)
                {
                    tf.reader.publish_prepared_pin(pin);
                }
            }
            self.claims.lock().extend(output_numbers.iter().copied());
            (
                CompactionClaim {
                    claims: self.claims.clone(),
                    numbers: output_numbers,
                },
                inner.versions.manifest_sync_handle(),
            )
        };
        confirm_manifest_durable(&manifest, &self.manifest_poisoned).ctx()?;
        {
            let mut inner = self.inner.lock();
            let oldest = inner
                .immutable_memtables
                .pop_back()
                .expect("pending flush generation");
            debug_assert!(Arc::ptr_eq(&oldest, &frozen.old_mem));
            inner.l0_install_epoch += 1;
            self.l0_file_count
                .store(inner.versions.current().l0_file_count(), Ordering::Relaxed);
            refresh_super_version(&self.super_version, &inner);
        }
        // Release the durable L0 claim before waking mmdb-compaction so an
        // immediate pick_l0_compaction does not see the new L0 SSTs in_flight.
        drop(claim);
        self.stats.record_flush();
        let old_wal = self
            .storage_path
            .join(format!("{:06}.wal", frozen.old_wal_number));
        if let Err(e) = fs::remove_file(&old_wal) {
            tracing::warn!("failed to remove old WAL {}: {}", old_wal.display(), e);
        }
        #[cfg(test)]
        if let Some(hook) = BEFORE_FLUSH_NOTIFY.with(|h| h.borrow_mut().take()) {
            hook(self);
        }
        *self.compaction_notify.0.lock() = true;
        self.compaction_notify.1.notify_one();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::FAIL_NEXT_SYNC;
    use crate::{DurabilityMode, ErrorKind};
    use std::sync::mpsc;

    fn options() -> DbOptions {
        DbOptions::default()
            .with_write_buffer_size(8 << 10)
            .with_max_immutable_memtables(2)
            .with_l0_compaction_trigger(usize::MAX)
    }

    fn pause_next_flush(
        db: &DB,
        inject_manifest_error: bool,
    ) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (started, running) = mpsc::channel();
        let (release, block) = mpsc::channel();
        db.flush_queue.on_next_build(move || {
            started.send(()).unwrap();
            block.recv_timeout(Duration::from_secs(10)).unwrap();
            FAIL_NEXT_SYNC.with(|f| f.set(inject_manifest_error));
        });
        (running, release)
    }

    #[test]
    fn a_waiting_flush_captures_its_generation_before_the_next_write_group() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(options(), dir.path()).unwrap();
        db.put(b"before", b"1").unwrap();
        // Park leadership with an active write buffer. Arrange both a flush
        // and a later write behind it before the group completes.
        db.write_queue.lock().leader_active = true;
        thread::scope(|scope| {
            let flush = scope.spawn(|| db.flush());
            while db.write_queue.lock().waiting_barriers == 0 {
                thread::yield_now();
            }
            let writer = scope.spawn(|| db.put(b"after", b"2"));
            while db.write_queue.lock().queue.is_empty() {
                thread::yield_now();
            }
            db.write_queue.lock().leader_active = false;
            db.write_cv.notify_all();
            flush.join().unwrap().unwrap();
            writer.join().unwrap().unwrap();
        });
        let inner = db.inner.lock();
        assert!(
            inner
                .active_memtable
                .get(b"before", MAX_SEQUENCE_NUMBER)
                .is_none()
        );
        assert_eq!(
            inner.active_memtable.get(b"after", MAX_SEQUENCE_NUMBER),
            Some(Some(b"2".to_vec()))
        );
    }

    #[test]
    fn memory_usage_follows_the_installed_flush_generation() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        db.put(b"first", b"held by the caller").unwrap();
        let retained = db.get_pinned(b"first").unwrap().unwrap();
        let initial = db.memory_usage();
        assert!(initial.active_memtable_bytes > 0);
        assert_eq!(initial.immutable_memtable_count, 0);
        assert_eq!(initial.immutable_memtable_bytes, 0);
        assert_eq!(initial.l0_pin_bytes, 0);

        let (running, release) = pause_next_flush(&db, false);
        thread::scope(|scope| {
            let flush = scope.spawn(|| db.flush());
            running.recv_timeout(Duration::from_secs(2)).unwrap();
            let parked = db.memory_usage();
            assert_eq!(parked.active_memtable_bytes, 0);
            assert_eq!(parked.immutable_memtable_count, 1);
            assert_eq!(
                parked.immutable_memtable_bytes,
                initial.active_memtable_bytes
            );

            db.put(b"second", b"next generation").unwrap();
            let active_bytes = db.memory_usage().active_memtable_bytes;
            assert!(active_bytes > 0);
            release.send(()).unwrap();
            flush.join().unwrap().unwrap();
            let installed = db.memory_usage();
            assert_eq!(installed.active_memtable_bytes, active_bytes);
            assert_eq!(installed.immutable_memtable_count, 0);
            assert_eq!(installed.immutable_memtable_bytes, 0);
            assert!(installed.l0_pin_bytes > 0);
        });

        db.close().unwrap();
        let closed = db.memory_usage();
        assert_eq!(closed.active_memtable_bytes, 0);
        assert_eq!(closed.immutable_memtable_count, 0);
        assert_eq!(closed.immutable_memtable_bytes, 0);
        assert_eq!(closed.l0_pin_bytes, 0);
        assert_eq!(&*retained, b"held by the caller");
    }

    fn active_wal(db: &DB) -> PathBuf {
        db.path()
            .join(format!("{:06}.wal", db.inner.lock().wal_number))
    }

    #[test]
    fn newest_immutable_wins_while_oldest_flush_is_parked() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(options(), dir.path()).unwrap();
        let (running, release) = pause_next_flush(&db, false);
        let old = vec![1; (8 << 10) + 1];
        let new = vec![2; (8 << 10) + 1];
        db.put(b"k", &old).unwrap();
        running.recv_timeout(Duration::from_secs(2)).unwrap();
        let snapshot = db.snapshot();
        db.put(b"k", &new).unwrap();
        assert_eq!(db.inner.lock().immutable_memtables.len(), 2);
        assert_eq!(db.get(b"k").unwrap(), Some(new.clone()));
        assert_eq!(
            db.get_with_options(&snapshot.read_options(), b"k").unwrap(),
            Some(old.clone())
        );
        let no_wait = WriteOptions::default().with_no_slowdown(true);
        let wal = active_wal(&db);
        let before = fs::read(&wal).unwrap();
        let seq = db.read.current_sequence();
        assert_eq!(
            db.put_with_options(&no_wait, b"blocked", b"v")
                .unwrap_err()
                .kind(),
            ErrorKind::Busy
        );
        assert_eq!(db.read.current_sequence(), seq);
        assert_eq!(active_wal(&db), wal);
        assert_eq!(fs::read(wal).unwrap(), before);
        assert_eq!(db.get(b"blocked").unwrap(), None);
        db.sync_wal().unwrap();
        assert_eq!(db.read.current_sequence(), seq);
        thread::scope(|s| {
            let (done, finished) = mpsc::channel();
            let database = &db;
            let handle = s.spawn(move || {
                database.flush().unwrap();
                done.send(()).unwrap();
            });
            assert!(finished.recv_timeout(Duration::from_millis(50)).is_err());
            release.send(()).unwrap();
            handle.join().unwrap();
        });
        assert!(db.inner.lock().immutable_memtables.is_empty());
        assert_eq!(
            db.get_with_options(&snapshot.read_options(), b"k").unwrap(),
            Some(old)
        );
        drop(snapshot);
        db.close().unwrap();
        assert_eq!(
            DB::open(options(), dir.path()).unwrap().get(b"k").unwrap(),
            Some(new)
        );
    }

    #[test]
    fn mixed_group_busy_member_leaves_only_the_accepted_neighbors_wal() {
        fn wait_for(mut predicate: impl FnMut() -> bool) -> bool {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !predicate() {
                if std::time::Instant::now() >= deadline {
                    return false;
                }
                thread::yield_now();
            }
            true
        }

        let dir = tempfile::tempdir().unwrap();
        let reference_dir = tempfile::tempdir().unwrap();
        let opts = DbOptions::default().with_max_immutable_memtables(1);
        let synced = WriteOptions::default().with_durability(DurabilityMode::WalSync);
        let reference = DB::open(opts.clone(), reference_dir.path()).unwrap();
        reference.put(b"seed", b"initial").unwrap();
        reference.flush().unwrap();
        reference
            .put_with_options(&synced, b"admitted", b"value")
            .unwrap();
        let expected_wal = fs::read(active_wal(&reference)).unwrap();
        assert!(!expected_wal.is_empty());

        let db = DB::open(opts.clone(), dir.path()).unwrap();
        db.put(b"seed", b"initial").unwrap();
        let before = db.snapshot().sequence();
        let (running, release) = pause_next_flush(&db, false);
        #[cfg(feature = "bench-metrics")]
        let (send_profile, profiles) = mpsc::channel();
        let submit = |options: &WriteOptions, key: &[u8], value: &[u8]| {
            let result = db.put_with_options(options, key, value);
            #[cfg(feature = "bench-metrics")]
            send_profile
                .send(crate::take_thread_write_profile())
                .unwrap();
            result
        };
        let no_wait = WriteOptions::default().with_no_slowdown(true);
        thread::scope(|scope| {
            // Both calls pass public admission before our synthetic leader freezes
            // the generation. The real group leader must recheck capacity.
            db.write_queue.lock().leader_active = true;
            let refused = scope.spawn(|| submit(&no_wait, b"refused", b"absent"));
            let admitted = scope.spawn(|| submit(&synced, b"admitted", b"value"));
            let queued = wait_for(|| db.write_queue.lock().queue.len() == 2);
            let freeze = db.submit_active();
            db.write_queue.lock().leader_active = false;
            db.write_cv.notify_all();
            freeze.unwrap();
            running.recv_timeout(Duration::from_secs(5)).unwrap();
            // The flush worker is parked in the build hook. Only the group leader
            // waits on changed, after recording Busy and before any WAL append.
            let leader_waiting = wait_for(|| db.flush_queue.changed.notify_one());
            let wal = active_wal(&db);
            let wal_before_release = fs::read(&wal).unwrap();
            let sequence_before_release = db.snapshot().sequence();
            let group_pending = !refused.is_finished() && !admitted.is_finished();
            release.send(()).unwrap();
            assert_eq!(refused.join().unwrap().unwrap_err().kind(), ErrorKind::Busy);
            admitted.join().unwrap().unwrap();
            assert!(
                queued,
                "both public calls must have passed initial admission"
            );
            assert!(
                leader_waiting,
                "the admitted neighbor must wait for capacity"
            );
            assert!(group_pending, "group completion preceded the flush release");
            assert!(wal_before_release.is_empty());
            assert_eq!(sequence_before_release, before);
            assert_eq!(active_wal(&db), wal);
            assert_eq!(fs::read(wal).unwrap(), expected_wal);
            #[cfg(feature = "bench-metrics")]
            {
                let a = profiles.recv().unwrap();
                let b = profiles.recv().unwrap();
                assert_eq!(a.queue_waits + b.queue_waits, 2);
                assert!(a.queue_wakeups + b.queue_wakeups >= 2);
                assert!(a.queue_wait_ns + b.queue_wait_ns > 0);
                assert_eq!(a.groups + b.groups, 1);
                assert_eq!(a.max_group_requests.max(b.max_group_requests), 2);
                assert!(a.immutable_waits + b.immutable_waits >= 1);
                assert!(a.immutable_wait_ns + b.immutable_wait_ns > 0);
            }
        });
        assert_eq!(db.snapshot().sequence(), before + 1);
        assert_eq!(db.get(b"refused").unwrap(), None);
        db.simulate_crash();
        reference.simulate_crash();
        for path in [dir.path(), reference_dir.path()] {
            let restored = DB::open(opts.clone(), path).unwrap();
            assert_eq!(restored.snapshot().sequence(), before + 1);
            assert_eq!(
                restored.get(b"admitted").unwrap().as_deref(),
                Some(&b"value"[..])
            );
            assert_eq!(restored.get(b"refused").unwrap(), None);
            assert_eq!(
                restored.get(b"seed").unwrap().as_deref(),
                Some(&b"initial"[..])
            );
        }
    }

    #[test]
    fn manifest_failure_keeps_both_generations_and_stops_close_before_freeze() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(options().with_max_immutable_memtables(3), dir.path()).unwrap();
        let (running, release) = pause_next_flush(&db, true);
        let first_wal = db.inner.lock().wal_number;
        db.put(b"first", &[1; (8 << 10) + 1]).unwrap();
        running.recv_timeout(Duration::from_secs(2)).unwrap();
        let second_wal = db.inner.lock().wal_number;
        db.put(b"second", &[2; (8 << 10) + 1]).unwrap();
        db.put(b"active", b"3").unwrap();
        let active_wal = db.inner.lock().wal_number;
        let target = db.flush_queue.target();
        release.send(()).unwrap();
        assert!(db.flush_queue.wait(target).is_err());
        let inner = db.inner.lock();
        assert_eq!(inner.immutable_memtables.len(), 2);
        assert_eq!(inner.versions.log_number(), second_wal);
        drop(inner);
        assert!(db.close().is_err());
        assert!(
            db.close().is_err(),
            "a second closer must observe failed durability"
        );
        assert_eq!(db.inner.lock().wal_number, active_wal);
        for number in [first_wal, second_wal, active_wal] {
            assert!(dir.path().join(format!("{number:06}.wal")).exists());
        }
        let db = DB::open(options(), dir.path()).unwrap();
        for (key, value) in [
            (b"first".as_slice(), vec![1; (8 << 10) + 1]),
            (b"second", vec![2; (8 << 10) + 1]),
            (b"active", vec![b'3']),
        ] {
            assert_eq!(db.get(key).unwrap(), Some(value));
        }
    }

    #[test]
    fn seal_and_fence_failures_preserve_wal_and_reject_further_writes() {
        for fence in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open(options(), dir.path()).unwrap();
            db.put(b"before", b"v").unwrap();
            let wal = db.inner.lock().wal_number;
            FAIL_NEXT_SYNC.with(|f| f.set(true));
            let failed = if fence { db.sync_wal() } else { db.flush() };
            assert_eq!(failed.unwrap_err().kind(), ErrorKind::Io);
            assert_eq!(
                db.put(b"later", b"v").unwrap_err().kind(),
                ErrorKind::Background
            );
            assert!(db.close().is_err());
            assert_eq!(db.inner.lock().wal_number, wal);
            assert!(dir.path().join(format!("{wal:06}.wal")).exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn power_loss_discards_only_the_unsynced_suffix_across_wal_rotation() {
        use crate::wal::writer::OBSERVE_SYNC_DATA;
        use std::collections::HashMap;
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let recovered = tempfile::tempdir().unwrap();
        let db = DB::open(options(), dir.path()).unwrap();
        let (running, release) = pause_next_flush(&db, false);
        // Model page-cache loss using only offsets observed after successful
        // data-sync syscalls. Buffered appends and flush() cannot advance them.
        let durable = Arc::new(Mutex::new(HashMap::new()));
        let observed = durable.clone();
        OBSERVE_SYNC_DATA.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |file, end| {
                observed.lock().insert(file.metadata().unwrap().ino(), end);
            }))
        });
        db.put(b"sealed", &[1; (8 << 10) + 1]).unwrap();
        running.recv_timeout(Duration::from_secs(2)).unwrap();
        db.put(b"fenced", b"2").unwrap();
        db.put(b"map-0001/gone", b"old").unwrap();
        let path = dir
            .path()
            .join(format!("{:06}.wal", db.inner.lock().wal_number));
        let end = fs::metadata(&path).unwrap().len();
        let active_inode = fs::metadata(&path).unwrap().ino();
        let seq = db.read.current_sequence();
        db.sync_wal().unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), end);
        assert_eq!(db.read.current_sequence(), seq);
        assert_eq!(durable.lock().get(&active_inode), Some(&end));

        let mut batch = WriteBatch::new();
        batch.delete_range(b"map-0001", b"map-0002");
        batch.put_prefixed(b"map-0001", b"/new", b"");
        batch.put_prefixed(b"map-0002", b"/ref", b"linked");
        db.write_with_options(
            &WriteOptions::default().with_durability(DurabilityMode::WalSync),
            batch,
        )
        .unwrap();
        let synced_end = fs::metadata(&path).unwrap().len();
        assert!(synced_end > end);
        assert_eq!(durable.lock().get(&active_inode), Some(&synced_end));
        db.put_with_options(
            &WriteOptions::default().with_durability(DurabilityMode::WalAsync),
            b"suffix",
            b"3",
        )
        .unwrap();
        db.put_with_options(
            &WriteOptions::default().with_durability(DurabilityMode::DisableWal),
            b"volatile",
            b"4",
        )
        .unwrap();
        OBSERVE_SYNC_DATA.with(|hook| hook.borrow_mut().take());
        for entry in fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let destination = recovered.path().join(entry.file_name());
            fs::copy(entry.path(), &destination).unwrap();
            if entry.path().extension().is_some_and(|ext| ext == "wal") {
                let durable_len = durable
                    .lock()
                    .get(&entry.metadata().unwrap().ino())
                    .copied()
                    .unwrap_or(0);
                OpenOptions::new()
                    .write(true)
                    .open(destination)
                    .unwrap()
                    .set_len(durable_len)
                    .unwrap();
            }
        }
        release.send(()).unwrap();
        for read_only in [true, false] {
            let survivor = if read_only {
                DB::open_read_only(recovered.path()).unwrap()
            } else {
                DB::open(options(), recovered.path()).unwrap()
            };
            assert_eq!(
                survivor.get(b"sealed").unwrap(),
                Some(vec![1; (8 << 10) + 1])
            );
            assert_eq!(survivor.get(b"fenced").unwrap().as_deref(), Some(&b"2"[..]));
            assert_eq!(survivor.get(b"map-0001/gone").unwrap(), None);
            assert_eq!(survivor.get(b"map-0001/new").unwrap(), Some(Vec::new()));
            assert_eq!(
                survivor.get(b"map-0002/ref").unwrap().as_deref(),
                Some(b"linked".as_slice())
            );
            assert_eq!(survivor.get(b"suffix").unwrap(), None);
            assert_eq!(survivor.get(b"volatile").unwrap(), None);
        }
    }
    #[test]
    fn l0_epoch_change_discards_unlocked_rewrite_and_moves_current_files_into_l1() {
        use crate::BlockPropertyCollector;
        type Gate = Arc<Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>>;
        struct Park(Gate);
        impl BlockPropertyCollector for Park {
            fn add(&mut self, _: &[u8]) {
                let gate = self.0.lock().take();
                if let Some((entered, release)) = gate {
                    entered.send(()).unwrap();
                    release.recv_timeout(Duration::from_secs(5)).unwrap();
                }
            }
            fn finish_block(&mut self) -> Vec<u8> {
                Vec::new()
            }
            fn name(&self) -> &str {
                "rewrite-gate"
            }
        }
        let gate: Gate = Arc::default();
        let factory_gate = gate.clone();
        let dir = tempfile::tempdir().unwrap();
        let options = options().with_block_property_collectors(vec![Arc::new(move || {
            Box::new(Park(factory_gate.clone()))
        })]);
        let db = DB::open(options.clone(), dir.path()).unwrap();
        db.put(b"k", b"old").unwrap();
        db.flush().unwrap();
        db.put(b"other", b"value").unwrap();
        db.flush().unwrap();
        let (entered, parked) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        *gate.lock() = Some((entered, resume));
        thread::scope(|scope| {
            let rewrite = scope.spawn(|| {
                let ctx = CompactionContext {
                    db_path: &db.storage_path,
                    dir_sync: &db.dir_sync,
                    manifest_poisoned: &db.read.manifest_poisoned,
                    options: &db.options,
                    builds: &db.builds,
                    stats: Some(&db.read.stats),
                    rate_limiter: None,
                    active_snapshots: &[],
                };
                db.level_rewrite().run(0, &ctx, false).unwrap()
            });
            parked.recv_timeout(Duration::from_secs(2)).unwrap();
            db.put(b"k", b"new").unwrap();
            db.flush().unwrap();
            assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"new".as_slice()));
            release.send(()).unwrap();
            assert!(!rewrite.join().unwrap());
        });
        assert_eq!(db.inner.lock().versions.current().l0_file_count(), 0);
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"new".as_slice()));
        db.close().unwrap();
        let db = DB::open(options, dir.path()).unwrap();
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"new".as_slice()));
    }
    #[test]
    fn flush_collector_can_read_and_receives_errors_for_self_waiting_operations() {
        use crate::BlockPropertyCollector;
        use std::sync::Weak;
        struct Calls {
            store: Arc<Mutex<Weak<DB>>>,
            checked: mpsc::Sender<()>,
        }
        impl BlockPropertyCollector for Calls {
            fn add(&mut self, key: &[u8]) {
                let db = self.store.lock().upgrade().unwrap();
                assert!(db.get(key).unwrap().is_some());
                let _snap = db.snapshot();
                for attempt in [
                    db.put(b"recursive", b"value"),
                    db.flush(),
                    db.sync_wal(),
                    db.compact_range(None, None),
                    db.compact(),
                    db.close(),
                ] {
                    assert_eq!(attempt.unwrap_err().kind(), ErrorKind::InvalidArgument);
                }
                self.checked.send(()).unwrap();
            }
            fn finish_block(&mut self) -> Vec<u8> {
                Vec::new()
            }
            fn name(&self) -> &str {
                "callback-test"
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Mutex::new(Weak::new()));
        let weak = store.clone();
        let (checked, received) = mpsc::channel();
        let db = Arc::new(
            DB::open(
                options()
                    .with_max_immutable_memtables(1)
                    .with_block_property_collectors(vec![Arc::new(move || {
                        Box::new(Calls {
                            store: weak.clone(),
                            checked: checked.clone(),
                        })
                    })]),
                dir.path(),
            )
            .unwrap(),
        );
        *store.lock() = Arc::downgrade(&db);
        db.put(b"k", &[1; (8 << 10) + 1]).unwrap();
        received.recv_timeout(Duration::from_secs(3)).unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"recursive").unwrap(), None);
    }
    #[test]
    fn directory_sync_failure_leaves_manifest_boundary_and_old_wal_intact() {
        use crate::manifest::version_set::FAIL_NEXT_SST_DIR_SYNC;
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(options(), dir.path()).unwrap();
        let old_wal = db.inner.lock().wal_number;
        db.flush_queue
            .on_next_build(|| FAIL_NEXT_SST_DIR_SYNC.set(true));
        db.put(b"key", b"value").unwrap();
        assert!(db.flush().is_err());
        assert!(db.read.manifest_poisoned.load(Ordering::Acquire));
        assert_eq!(db.inner.lock().versions.log_number(), old_wal);
        assert!(dir.path().join(format!("{old_wal:06}.wal")).is_file());
        assert!(db.close().is_err());
        let db = DB::open(options(), dir.path()).unwrap();
        assert_eq!(
            db.get(b"key").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
    }

    #[test]
    fn failed_wal_creation_keeps_the_sealed_writer_and_generation() {
        let dir = tempfile::tempdir().unwrap();
        // Keep this write below the standby threshold so flush creates the next WAL itself.
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        db.put(b"key", b"value").unwrap();
        let old_wal = db.inner.lock().wal_number;
        let occupied = dir.path().join(format!(
            "{:06}.wal",
            db.inner.lock().versions.next_file_number()
        ));
        fs::create_dir(&occupied).unwrap();
        assert_eq!(db.flush().unwrap_err().kind(), ErrorKind::Io);
        {
            let inner = db.inner.lock();
            assert_eq!(inner.wal_number, old_wal);
            assert!(inner.wal_writer.is_some());
            assert!(inner.immutable_memtables.is_empty());
        }
        assert_eq!(db.flush_queue.target(), 0);
        assert!(db.close().is_err());
        fs::remove_dir(occupied).unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        assert_eq!(
            db.get(b"key").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
    }

    #[test]
    fn auto_freeze_failure_still_acknowledges_committed_write_group() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(options(), dir.path()).unwrap();
        let occupied = dir.path().join(format!(
            "{:06}.wal",
            db.inner.lock().versions.next_file_number()
        ));
        fs::create_dir(&occupied).unwrap();

        let payload = vec![7u8; (8 << 10) + 64];
        let mut batch = WriteBatch::new();
        batch.put(b"committed", &payload);
        db.write_with_options(
            &WriteOptions::default().with_durability(DurabilityMode::WalSync),
            batch,
        )
        .expect("committed group must return Ok even when post-commit rotation fails");

        assert_eq!(
            db.get(b"committed").unwrap_err().kind(),
            ErrorKind::Background
        );
        assert_eq!(
            db.put(b"after", b"v").unwrap_err().kind(),
            ErrorKind::Background
        );
        assert_eq!(db.close().unwrap_err().kind(), ErrorKind::Background);

        fs::remove_dir(&occupied).unwrap();
        let reopened = DB::open(options(), dir.path()).unwrap();
        assert_eq!(
            reopened.get(b"committed").unwrap().as_deref(),
            Some(payload.as_slice())
        );
    }

    #[test]
    fn bg_error_is_published_before_waking_flush_queue_waiters() {
        // 1. Foreground DB::set_bg_error path.
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(DB::open(options(), dir.path()).unwrap());
        db.put(b"k", b"v").unwrap();
        let db_check = db.clone();
        let (tx, rx) = mpsc::channel();
        db.flush_queue.on_next_fail(move || {
            let _ = tx.send(db_check.get(b"k").map_err(|e| e.kind()));
        });
        db.set_bg_error("simulated foreground error".to_string());
        assert_eq!(rx.recv().unwrap(), Err(ErrorKind::Background));

        // 2. Background compaction worker set_error path.
        let dir2 = tempfile::tempdir().unwrap();
        let opts2 = DbOptions::default()
            .with_write_buffer_size(8 << 10)
            .with_l0_compaction_trigger(2)
            .with_pin_l0_first_block_and_warm_index(false);
        let db2 = Arc::new(DB::open(opts2, dir2.path()).unwrap());
        db2.put(b"a", b"1").unwrap();
        db2.flush().unwrap();
        let sst_num = db2.inner.lock().versions.current().level_files(0)[0]
            .meta
            .number;
        fs::write(dir2.path().join(format!("{sst_num:06}.sst")), b"corrupt").unwrap();
        let db2_check = db2.clone();
        let (tx2, rx2) = mpsc::channel();
        db2.flush_queue.on_next_fail(move || {
            let _ = tx2.send(db2_check.get(b"a").map_err(|e| e.kind()));
        });
        db2.put(b"b", &[2u8; (8 << 10) + 64]).unwrap();
        db2.flush_queue.wait(db2.flush_queue.target()).unwrap();
        db2.read.signal_compaction();
        assert_eq!(
            rx2.recv_timeout(Duration::from_secs(3)).unwrap(),
            Err(ErrorKind::Background)
        );
    }

    #[test]
    fn flush_releases_l0_claim_before_signaling_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions::default()
            .with_write_buffer_size(8 << 10)
            .with_l0_compaction_trigger(1);
        let db = DB::open(opts, dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        db.flush_queue.on_next_build(move || {
            BEFORE_FLUSH_NOTIFY.with(|h| {
                *h.borrow_mut() = Some(Box::new(move |ctx| {
                    let version = ctx.inner.lock().versions.current();
                    let in_flight = ctx.claims.lock().clone();
                    let picked =
                        LeveledCompaction::pick_compaction(&version, &ctx.options, &in_flight)
                            .is_some();
                    tx.send((in_flight.is_empty(), picked)).unwrap();
                }));
            });
        });
        db.put(b"k", &[1u8; (8 << 10) + 64]).unwrap();
        let (claims_empty, pickable) = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(
            claims_empty,
            "new L0 claim must drop before compaction_notify"
        );
        assert!(pickable, "woken compaction worker must be able to pick L0");
    }

    #[test]
    fn standby_wal_rotates_without_leader_create_and_unlinks_unused_on_close() {
        use crate::wal::writer::WAL_NEW_CALLS;

        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(options(), dir.path()).unwrap();
        WAL_NEW_CALLS.with(|n| n.set(0));

        // Crossing 50% of write_buffer_size (8 KiB -> >= 4 KiB) arms background
        // zero-length standby WAL creation on mmdb-flush.
        db.put(b"half", &[b'a'; 4500]).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !db.flush_queue.has_standby() {
            assert!(
                std::time::Instant::now() < deadline,
                "mmdb-flush did not prepare standby WAL in time"
            );
            thread::sleep(Duration::from_millis(5));
        }

        // Crossing 100% triggers freeze_memtable(), which swaps in the standby
        // WalWriter in O(1) with zero WalWriter::new calls on the leader thread.
        db.put(b"full", &[b'b'; 4500]).unwrap();
        assert_eq!(
            WAL_NEW_CALLS.with(|n| n.get()),
            0,
            "rotation leader must consume standby WAL without calling WalWriter::new"
        );
        db.flush_queue.wait(db.flush_queue.target()).unwrap();

        // Prepare another standby WAL on an empty active memtable, then close():
        // close() must unlink the unused standby WAL so no orphan .wal remains.
        db.flush_queue.request_standby();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !db.flush_queue.has_standby() {
            assert!(std::time::Instant::now() < deadline);
            thread::sleep(Duration::from_millis(5));
        }
        let live_wal = db.inner.lock().wal_number;
        db.close().unwrap();

        let wals: Vec<u64> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| {
                let name = e.ok()?.file_name().into_string().ok()?;
                name.strip_suffix(".wal")?.parse::<u64>().ok()
            })
            .collect();
        assert_eq!(
            wals,
            vec![live_wal],
            "close() must unlink any unused standby WAL"
        );

        let ro = DB::open_read_only(dir.path()).unwrap();
        assert_eq!(ro.get(b"half").unwrap().map(|v| v.len()), Some(4500));
        assert_eq!(ro.get(b"full").unwrap().map(|v| v.len()), Some(4500));
    }

    #[test]
    fn crash_with_empty_standby_wal_recovers_torn_active_tail_and_cleans_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(options(), dir.path()).unwrap();
        let active_wal = db.inner.lock().wal_number;

        // Commit durable data + cross 50% so a higher-numbered 0-byte standby WAL
        // exists on disk alongside the active WAL.
        db.put_with_options(
            &WriteOptions::default().with_durability(DurabilityMode::WalSync),
            b"durable",
            &[b'x'; 4500],
        )
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !db.flush_queue.has_standby() {
            assert!(std::time::Instant::now() < deadline);
            thread::sleep(Duration::from_millis(5));
        }
        let standby_wal = db.flush_queue.standby_wal.lock().as_ref().unwrap().0;
        assert!(standby_wal > active_wal);

        db.simulate_crash();

        let active_path = dir.path().join(format!("{active_wal:06}.wal"));
        let standby_path = dir.path().join(format!("{standby_wal:06}.wal"));
        assert!(active_path.is_file());
        assert_eq!(
            fs::metadata(&standby_path).unwrap().len(),
            0,
            "standby WAL must be 0 bytes on disk across crash"
        );

        // Append a torn 3-byte partial header to the active WAL: recovery must
        // still identify `active_wal` (not the 0-byte `standby_wal`) as
        // `recoverable_tail_wal_num`.
        {
            use std::io::Write;
            let mut f = fs::OpenOptions::new()
                .append(true)
                .open(&active_path)
                .unwrap();
            f.write_all(&[0xde, 0xad, 0xbe]).unwrap();
        }

        // Read-only open recovers cleanly without deleting the 0-byte standby WAL.
        let ro = DB::open_read_only(dir.path()).unwrap();
        assert_eq!(ro.get(b"durable").unwrap().map(|v| v.len()), Some(4500));
        drop(ro);
        assert!(standby_path.exists());

        // Writable open recovers cleanly and unlinks the 0-byte orphan standby WAL.
        let reopened = DB::open(options(), dir.path()).unwrap();
        assert_eq!(
            reopened.get(b"durable").unwrap().map(|v| v.len()),
            Some(4500)
        );
        assert!(
            !standby_path.exists(),
            "writable recovery must unlink the unused 0-byte standby WAL"
        );
        reopened.close().unwrap();
    }
}
