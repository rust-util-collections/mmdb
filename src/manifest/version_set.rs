//! VersionSet: manages the MANIFEST file and the chain of Versions.

use std::{
    collections::HashMap,
    fs,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::cache::table_cache::TableCache;
use crate::error::{Error, Result, ResultExt};
use crate::manifest::version::{TableFile, Version};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::sst::table_reader::TableReader;
use crate::types::{MAX_SEQUENCE_NUMBER, SequenceNumber, compare_internal_key};
use crate::wal::{WalReader, WalWriter};
use parking_lot::Mutex;

/// Manages the MANIFEST file and the current Version.
pub struct VersionSet {
    db_path: PathBuf,
    num_levels: usize,

    /// The current version (immutable snapshot).
    current: Arc<Version>,

    /// Next file number to allocate.
    next_file_number: u64,
    /// Current WAL log number.
    log_number: u64,
    /// Last sequence number used.
    last_sequence: SequenceNumber,
    /// Current MANIFEST file number.
    manifest_number: u64,
    /// MANIFEST writer, behind its own lock so `sync` can be called
    /// without holding the main DB mutex.
    manifest_writer: Arc<Mutex<Option<WalWriter>>>,
    /// Table cache for opening SST readers.
    table_cache: Option<Arc<TableCache>>,
    /// Count of edits since last MANIFEST snapshot (for Phase I compaction).
    edits_since_snapshot: u64,
    /// Set when the MANIFEST writer is in an unrecoverable state: a record
    /// append failed mid-write (leaving a torn record in the stream —
    /// appending more records after it would make the file unrecoverable
    /// mid-file), a sync could not confirm durability of appended records,
    /// or a rotation published a new MANIFEST whose durability could not be
    /// confirmed. While poisoned, `log_and_apply` fails fast *before*
    /// applying anything, so callers can rely on the invariant "Err ⇒ edit
    /// not applied". Recovery on reopen truncates any torn tail.
    ///
    /// Shared (`Arc`) so the DB can observe poisoning lock-free in
    /// `check_usable` and poison it from `manifest_sync_handle` sync sites
    /// that bypass [`Self::sync_manifest`].
    poisoned: Arc<AtomicBool>,
}

impl VersionSet {
    fn parse_manifest_number(manifest_name: &str) -> Result<u64> {
        let digits = manifest_name
            .strip_prefix("MANIFEST-")
            .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .ok_or_else(|| {
                Error::corruption(format!(
                    "CURRENT contains invalid MANIFEST name: {manifest_name:?}"
                ))
            })?;
        let manifest_number = digits.parse::<u64>().map_err(|_| {
            Error::corruption(format!(
                "CURRENT MANIFEST number is out of range: {manifest_name:?}"
            ))
        })?;
        let canonical_name = format!("MANIFEST-{manifest_number:06}");
        if manifest_name != canonical_name {
            return Err(Error::corruption(format!(
                "CURRENT contains non-canonical MANIFEST name: {manifest_name:?}"
            )));
        }
        Ok(manifest_number)
    }

    /// Create a new VersionSet (for a fresh database). Test-only wrapper.
    #[cfg(test)]
    pub(crate) fn create(db_path: &Path, num_levels: usize) -> Result<Self> {
        Self::create_with_cache(db_path, num_levels, None)
    }

    /// Create a new VersionSet with an optional table cache.
    pub fn create_with_cache(
        db_path: &Path,
        num_levels: usize,
        table_cache: Option<Arc<TableCache>>,
    ) -> Result<Self> {
        let manifest_number = 1;
        let manifest_path = db_path.join(format!("MANIFEST-{:06}", manifest_number));
        let manifest_writer = WalWriter::new(&manifest_path).ctx()?;

        // Write CURRENT file
        Self::set_current_file(db_path, manifest_number).ctx()?;

        let mut vs = Self {
            db_path: db_path.to_path_buf(),
            num_levels,
            current: Arc::new(Version::new(num_levels)),
            next_file_number: 2, // 1 is used by MANIFEST
            log_number: 0,
            last_sequence: 0,
            manifest_number,
            manifest_writer: Arc::new(Mutex::new(Some(manifest_writer))),
            table_cache,
            edits_since_snapshot: 0,
            poisoned: Arc::new(AtomicBool::new(false)),
        };

        // Write initial snapshot edit
        let mut edit = VersionEdit::new();
        edit.set_next_file_number(vs.next_file_number);
        edit.set_last_sequence(vs.last_sequence);
        vs.log_and_apply(edit).ctx()?;
        vs.sync_manifest().ctx()?;

        Ok(vs)
    }

    /// Test-only wrapper for [`Self::recover_with_cache`].
    #[cfg(test)]
    pub(crate) fn recover(db_path: &Path, num_levels: usize) -> Result<Self> {
        Self::recover_with_cache(db_path, num_levels, None)
    }

    /// Recover with an optional table cache.
    pub fn recover_with_cache(
        db_path: &Path,
        num_levels: usize,
        table_cache: Option<Arc<TableCache>>,
    ) -> Result<Self> {
        // Read CURRENT to find manifest file
        let current_path = db_path.join("CURRENT");
        let manifest_name = fs::read_to_string(&current_path)
            .map_err(|e| Error::corruption(format!("cannot read CURRENT: {}", e)))
            .ctx()?;
        let manifest_name = manifest_name.trim_end_matches(['\r', '\n']);
        let manifest_number = Self::parse_manifest_number(manifest_name).ctx()?;
        let manifest_path = db_path.join(format!("MANIFEST-{manifest_number:06}"));

        // Replay MANIFEST records in two passes:
        // Pass 1: Collect all edits to determine the final file set.
        // Pass 2: Open only the files that survive all deletions.
        // This handles the case where compaction adds + deletes files in
        // separate edits, and the deleted SST no longer exists on disk.
        let mut reader = WalReader::new(&manifest_path).ctx()?;
        let mut next_file_number = 2u64;
        let mut log_number = 0u64;
        let mut last_sequence = 0u64;

        // (level, meta) pairs for files that are still live after all edits.
        let mut live_files: HashMap<u64, (usize, FileMetaData)> = HashMap::new();
        // Number of edits replayed — seeds `edits_since_snapshot` so that
        // restart-heavy workloads still trigger MANIFEST compaction instead
        // of growing the file without bound across process lifetimes.
        let mut edits_replayed = 0u64;

        loop {
            let data = match reader.read_record() {
                Ok(Some(data)) => data,
                Ok(None) => break,
                // Tolerate a corrupt record only when nothing but zero padding
                // follows it (torn tail); `open_append_truncated` below then
                // discards the tail. Corruption followed by real data would
                // silently drop later edits — fail the open instead.
                Err(e) if reader.rest_is_zero_padding().unwrap_or(false) => {
                    tracing::warn!("MANIFEST {} has corrupt tail: {}", manifest_name, e);
                    break;
                }
                Err(e) => return Err(e),
            };
            let edit = VersionEdit::decode(&data).ctx()?;
            edits_replayed += 1;

            // Forward-only, mirroring `log_and_apply`'s bookkeeping: replayed
            // edits are chronological, so a lower value is necessarily stale
            // and taking it would re-issue already-consumed file numbers.
            if let Some(n) = edit.next_file_number {
                next_file_number = next_file_number.max(n);
            }
            if let Some(n) = edit.log_number {
                log_number = n;
            }
            // Same forward-only rule: a regressed sequence would make new
            // writes lose MVCC precedence to already-persisted entries. A
            // value above the 56-bit packing limit can only come from a
            // malformed record — fail loudly instead of seeding a counter
            // that would overflow the internal-key trailer.
            if let Some(s) = edit.last_sequence {
                if s > MAX_SEQUENCE_NUMBER {
                    return Err(Error::corruption(format!(
                        "MANIFEST last_sequence {} exceeds maximum {}",
                        s, MAX_SEQUENCE_NUMBER
                    )));
                }
                last_sequence = last_sequence.max(s);
            }

            // Track deletions FIRST — a trivial-move edit deletes from the
            // old level and adds to the new level with the same file number.
            // Processing additions first would insert then immediately remove
            // the file, losing it entirely.
            //
            // Deletions must match the recorded level: `log_and_apply` only
            // removes a file from the exact level named in the edit, so a
            // level-mismatched (or non-live) deletion record means the
            // MANIFEST no longer reflects a consistent file set. Silently
            // dropping by number here would diverge from the in-process
            // behavior and could unlink a live file after restart.
            for (level, file_number) in &edit.deleted_files {
                match live_files.get(file_number) {
                    Some((live_level, _)) if *live_level == *level as usize => {
                        live_files.remove(file_number);
                    }
                    Some((live_level, _)) => {
                        return Err(Error::corruption(format!(
                            "MANIFEST deletes SST {} at level {} but it is live at level {}",
                            file_number, level, live_level
                        )));
                    }
                    None => {
                        return Err(Error::corruption(format!(
                            "MANIFEST deletes SST {} at level {} but it is not live",
                            file_number, level
                        )));
                    }
                }
            }

            // Track additions
            for (level, meta) in &edit.new_files {
                let level = *level as usize;
                if level >= num_levels {
                    // Silently dropping the file would make its keys unreadable,
                    // and the next MANIFEST snapshot would persist the loss
                    // permanently. Fail loudly instead.
                    return Err(Error::corruption(format!(
                        "MANIFEST references SST {} at level {} but the database \
                         is configured with num_levels {}; reopen with num_levels >= {}",
                        meta.number,
                        level,
                        num_levels,
                        level + 1
                    )));
                }
                if let Some((prev_level, _)) = live_files.insert(meta.number, (level, meta.clone()))
                {
                    return Err(Error::corruption(format!(
                        "MANIFEST adds SST {} at level {} but it is already live at level {}",
                        meta.number, level, prev_level
                    )));
                }
            }
        }

        // Pass 2: Open only live files
        let mut version = Version::new(num_levels);
        for (level, meta) in live_files.values() {
            let reader_result = if let Some(ref tc) = table_cache {
                tc.get_reader(meta.number)
            } else {
                let sst_path = db_path.join(format!("{:06}.sst", meta.number));
                TableReader::open(&sst_path).map(Arc::new)
            };
            match reader_result {
                Ok(reader) => {
                    version.files[*level].push(TableFile {
                        meta: meta.clone(),
                        reader,
                    });
                }
                Err(e) => {
                    return Err(Error::corruption(format!(
                        "cannot open SST {} during recovery: {}",
                        meta.number, e
                    )));
                }
            }
        }

        // Sort L0 by file number descending (newest first)
        version.files[0].sort_by_key(|file| std::cmp::Reverse(file.meta.number));
        // Sort L1+ by smallest key
        for level in 1..num_levels {
            version.files[level]
                .sort_by(|a, b| compare_internal_key(&a.meta.smallest_key, &b.meta.smallest_key));
        }

        // Reopen manifest for appending, truncating any corrupt tail
        let valid_offset = reader.last_valid_offset();
        let manifest_writer =
            WalWriter::open_append_truncated(&manifest_path, valid_offset).ctx()?;

        Ok(Self {
            db_path: db_path.to_path_buf(),
            num_levels,
            current: Arc::new(version),
            next_file_number,
            log_number,
            last_sequence,
            manifest_number,
            manifest_writer: Arc::new(Mutex::new(Some(manifest_writer))),
            table_cache,
            edits_since_snapshot: edits_replayed,
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Open or create a VersionSet with an optional table cache.
    pub fn open_with_cache(
        db_path: &Path,
        num_levels: usize,
        table_cache: Option<Arc<TableCache>>,
    ) -> Result<Self> {
        let current_path = db_path.join("CURRENT");
        if current_path.exists() {
            Self::recover_with_cache(db_path, num_levels, table_cache)
        } else {
            Self::create_with_cache(db_path, num_levels, table_cache)
        }
    }

    /// Apply a VersionEdit: write to MANIFEST and install a new Version.
    ///
    /// Error contract: `Err` means the edit was **not** applied — neither in
    /// memory nor durably. Callers rely on this to safely delete output SSTs
    /// referenced by a failed edit.
    pub fn log_and_apply(&mut self, edit: VersionEdit) -> Result<()> {
        if self.is_poisoned() {
            return Err(Error::corruption(
                "MANIFEST writer poisoned by an earlier write failure; \
                 reopen the database to recover"
                    .to_string(),
            ));
        }

        // Build new version from current + edit FIRST, before persisting.
        // This ensures that if an SST fails to open, the MANIFEST is not
        // polluted with an edit referencing a broken file.
        let mut new_version = (*self.current).clone();

        for (level, meta) in &edit.new_files {
            let level = *level as usize;
            // An out-of-range level is an internal logic bug. Silently skipping
            // would persist an edit whose keys become unreadable and whose
            // replay is rejected by `recover_with_cache` on the next open —
            // fail loudly before anything is written.
            if level >= self.num_levels {
                return Err(Error::corruption(format!(
                    "VersionEdit adds SST {} at out-of-range level {} (num_levels {})",
                    meta.number, level, self.num_levels
                )));
            }
            let reader = if let Some(ref tc) = self.table_cache {
                tc.get_reader(meta.number)
            } else {
                let sst_path = self.db_path.join(format!("{:06}.sst", meta.number));
                TableReader::open(&sst_path).map(Arc::new)
            };
            match reader {
                Ok(reader) => {
                    if level == 0 {
                        // L0: insert at front (newest first)
                        new_version.files[level].insert(
                            0,
                            TableFile {
                                meta: meta.clone(),
                                reader,
                            },
                        );
                    } else {
                        new_version.files[level].push(TableFile {
                            meta: meta.clone(),
                            reader,
                        });
                        // Keep levels 1+ sorted by smallest key (using logical ordering)
                        new_version.files[level].sort_by(|a, b| {
                            compare_internal_key(&a.meta.smallest_key, &b.meta.smallest_key)
                        });
                    }
                }
                Err(e) => {
                    // Nothing has been persisted or applied at this point —
                    // the edit is rejected wholesale. Include an existence
                    // probe so a post-mortem can distinguish "file vanished"
                    // (unlinked between build and install) from "file present
                    // but unreadable" (open raced a transient failure).
                    let sst_path = self.db_path.join(format!("{:06}.sst", meta.number));
                    return Err(Error::corruption(format!(
                        "failed to open new SST {} (path {}, exists_now={}): {}",
                        meta.number,
                        sst_path.display(),
                        sst_path.exists(),
                        e
                    )));
                }
            }
        }

        // Deletions must name a live file at the exact level. A miss means the
        // edit was built against a stale version (internal logic bug); recovery
        // replay enforces the same invariant, so persisting the edit would make
        // the next open fail. Fail loudly before anything is written.
        for (level, file_number) in &edit.deleted_files {
            let level = *level as usize;
            if level >= self.num_levels {
                return Err(Error::corruption(format!(
                    "VersionEdit deletes SST {} at out-of-range level {} (num_levels {})",
                    file_number, level, self.num_levels
                )));
            }
            let before = new_version.files[level].len();
            new_version.files[level].retain(|f| f.meta.number != *file_number);
            if new_version.files[level].len() == before {
                return Err(Error::corruption(format!(
                    "VersionEdit deletes SST {} at level {} but no such live file exists there",
                    file_number, level
                )));
            }
        }

        // Before any MANIFEST record referencing the new SST files can become
        // durable — either via a later `sync_manifest()`/`manifest_sync_handle`
        // sync, or via the `maybe_compact_manifest()` snapshot below — fsync the
        // DB directory so the new files' directory entries survive a crash.
        // (The file *contents* are already fsynced by `TableBuilder::finish`.)
        // Otherwise a crash could leave the MANIFEST referencing an SST whose
        // directory entry was lost.
        if !edit.new_files.is_empty()
            && let Err(e) = Self::fsync_directory(&self.db_path)
        {
            // A failed directory fsync means these files' directory entries
            // are not confirmed durable (fsyncgate): a later "successful"
            // fsync of the same directory (e.g. from a sibling background
            // compaction thread, since this is the only durability guard for
            // SST directory entries) could falsely report durability of the
            // entries this one failed to persist. Poison the writer so no
            // further edits are appended; reopen to recover.
            self.poisoned.store(true, Ordering::Release);
            return Err(e).ctx();
        }

        // Version built successfully — now persist to MANIFEST (write only, no sync).
        // Callers on hot paths should release the main lock and call
        // `sync_manifest()` separately to avoid stalling other operations.
        let encoded = edit.encode();
        {
            let mut w = self.manifest_writer.lock();
            if let Some(ref mut writer) = *w
                && let Err(e) = writer.add_record(&encoded)
            {
                // The failed append may have left a torn record in the
                // MANIFEST stream. Appending further records after it would
                // produce mid-file corruption that recovery rejects (only
                // *tail* corruption is tolerated). Poison the writer so no
                // more records can follow; recovery truncates the torn tail.
                drop(w);
                self.poisoned.store(true, Ordering::Release);
                return Err(e).ctx();
            }
        }

        // Update bookkeeping. Forward-only: file numbers are never reused,
        // so an edit carrying a stale (lower) counter value must never move
        // the allocator backwards — a regression would re-issue numbers of
        // files that may still exist on disk or be referenced by the version.
        if let Some(n) = edit.next_file_number {
            self.next_file_number = self.next_file_number.max(n);
        }
        if let Some(n) = edit.log_number {
            self.log_number = n;
        }
        // Forward-only for the same reason: a stale lower sequence would let
        // newly-assigned sequence numbers collide with already-committed
        // entries and lose MVCC precedence to them.
        if let Some(s) = edit.last_sequence {
            self.last_sequence = self.last_sequence.max(s);
        }

        self.current = Arc::new(new_version);
        self.edits_since_snapshot += 1;

        // Check if MANIFEST needs compaction. The edit is already applied at
        // this point, so `maybe_compact_manifest` must never surface an error
        // (callers would interpret it as "edit not applied" and e.g. delete
        // freshly-installed SSTs). All rotation failures are either deferred
        // (retried on a later edit) or poison the writer (fail-stop).
        self.maybe_compact_manifest();

        Ok(())
    }

    /// Sync the MANIFEST writer. Only acquires the internal manifest lock,
    /// so callers can invoke this without holding the main DB mutex.
    ///
    /// Directory entries for any newly created SST files are made durable in
    /// `log_and_apply` (before the referencing MANIFEST record), so this only
    /// needs to fsync the MANIFEST writer itself.
    pub fn sync_manifest(&self) -> Result<()> {
        let mut w = self.manifest_writer.lock();
        if let Some(ref mut writer) = *w
            && let Err(e) = writer.sync()
        {
            // A failed fsync may leave dirty pages marked clean (fsyncgate):
            // a later "successful" sync would falsely report durability of
            // the records that this sync failed to persist. Poison the writer
            // so no further records are appended; reopen to recover.
            drop(w);
            self.poisoned.store(true, Ordering::Release);
            return Err(e).ctx();
        }
        Ok(())
    }

    /// Return a handle for syncing the MANIFEST outside the main DB lock.
    /// Clone the Arc, release the DB lock, then call `lock() → sync()`.
    pub fn manifest_sync_handle(&self) -> Arc<Mutex<Option<WalWriter>>> {
        Arc::clone(&self.manifest_writer)
    }

    /// Allocate a new file number.
    pub fn new_file_number(&mut self) -> u64 {
        let n = self.next_file_number;
        self.next_file_number += 1;
        n
    }

    /// Reserve `count` consecutive file numbers. Returns the first number.
    /// Used to pre-allocate file numbers before releasing the lock for I/O.
    pub fn reserve_file_numbers(&mut self, count: u64) -> u64 {
        let start = self.next_file_number;
        self.next_file_number += count;
        start
    }

    /// Get the current version.
    pub fn current(&self) -> Arc<Version> {
        self.current.clone()
    }

    pub fn next_file_number(&self) -> u64 {
        self.next_file_number
    }

    /// Ensure `next_file_number` is at least `min_next`.
    /// Used after sub-compaction to guarantee no collision with numbers
    /// consumed by the atomic counter during parallel I/O.
    pub fn ensure_file_number_at_least(&mut self, min_next: u64) {
        if min_next > self.next_file_number {
            self.next_file_number = min_next;
        }
    }

    pub fn log_number(&self) -> u64 {
        self.log_number
    }

    /// Number of the MANIFEST file currently being appended to.
    pub fn manifest_number(&self) -> u64 {
        self.manifest_number
    }

    /// Whether the MANIFEST writer is poisoned (a record append failed
    /// mid-write, a sync could not confirm durability, or a rotation's
    /// durability could not be confirmed). While poisoned, every
    /// `log_and_apply` fails fast without applying anything; only a reopen
    /// recovers. Callers use this to distinguish a fail-stop condition from
    /// a cleanly-rejected (retryable) edit.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Shared handle to the poison flag. The DB stores this to observe
    /// poisoning lock-free in `check_usable`, and to poison the writer from
    /// sync sites that go through [`Self::manifest_sync_handle`] rather than
    /// [`Self::sync_manifest`].
    pub fn poison_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.poisoned)
    }

    pub fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence
    }

    /// Rewrite the MANIFEST with a full snapshot of the current version.
    /// This prevents unbounded MANIFEST growth.
    ///
    /// Called from `log_and_apply` *after* the edit has been applied, so this
    /// must never surface an error. Pre-publish failures on the *new* file are
    /// deferred (the file is discarded and the rotation retried on a later
    /// edit). Failures that compromise the writer that stays in service poison
    /// it instead: a failed old-writer sync (a later sync could falsely report
    /// durability of records this one failed to persist), or a post-publish
    /// directory-fsync failure — at that point both the old and the new
    /// MANIFEST durably contain every applied edit, but a crash could recover
    /// either one, so blocking further edits keeps the two from diverging
    /// (the old MANIFEST is retained as a fallback).
    pub fn maybe_compact_manifest(&mut self) {
        const MANIFEST_COMPACTION_THRESHOLD: u64 = 1000;

        if self.edits_since_snapshot < MANIFEST_COMPACTION_THRESHOLD {
            return;
        }

        // Allocate new manifest number BEFORE taking the snapshot,
        // so the snapshot's next_file_number accounts for the manifest file.
        let new_manifest_number = self.next_file_number;
        self.next_file_number += 1;

        let snapshot_edit = VersionEdit::from_version_snapshot(
            &self.current,
            self.log_number,
            self.next_file_number,
            self.last_sequence,
        );

        let new_manifest_path = self
            .db_path
            .join(format!("MANIFEST-{:06}", new_manifest_number));
        let mut new_writer = match WalWriter::new(&new_manifest_path).ctx() {
            Ok(writer) => writer,
            Err(e) => {
                // WalWriter::new creates (and truncates) the file before its
                // own parent-directory fsync, so a failure here (e.g. that
                // fsync failing) can still leave an empty file on disk —
                // clean it up like every other failure branch below.
                let _ = fs::remove_file(&new_manifest_path);
                tracing::warn!("MANIFEST compaction deferred creating snapshot: {}", e);
                return;
            }
        };

        // Write the snapshot edit
        let encoded = snapshot_edit.encode();
        if let Err(e) = new_writer.add_record(&encoded).ctx() {
            drop(new_writer);
            let _ = fs::remove_file(&new_manifest_path);
            tracing::warn!("MANIFEST compaction deferred writing snapshot: {}", e);
            return;
        }
        if let Err(e) = new_writer.sync().ctx() {
            drop(new_writer);
            let _ = fs::remove_file(&new_manifest_path);
            tracing::warn!("MANIFEST compaction deferred syncing snapshot: {}", e);
            return;
        }

        // Sync the OLD writer before publishing the new MANIFEST. The current
        // edit may still sit in the old writer's buffer; if the CURRENT rename
        // below turns out not to be durable (post-publish dir-fsync failure +
        // crash), recovery falls back to the old MANIFEST — which must then
        // contain everything `log_and_apply` reported as applied.
        {
            let mut w = self.manifest_writer.lock();
            if let Some(ref mut writer) = *w
                && let Err(e) = writer.sync()
            {
                drop(w);
                drop(new_writer);
                let _ = fs::remove_file(&new_manifest_path);
                // Unlike the new-writer failures above (whose file is simply
                // discarded), this fsync failed on the writer that stays in
                // service: a later sync of it could falsely report durability
                // of the records this one failed to persist (fsyncgate).
                // Poison instead of deferring.
                self.poisoned.store(true, Ordering::Release);
                tracing::error!(
                    "MANIFEST compaction could not sync the old manifest; \
                     poisoning manifest writer (reopen the database to recover): {}",
                    e
                );
                return;
            }
        }

        // Publish CURRENT in two phases so a post-rename fsync failure cannot
        // leave the live process appending to a different MANIFEST than CURRENT.
        // The temp file is named per-attempt (suffixed with new_manifest_number,
        // which is freshly allocated above) so a retry after a deferred failure
        // never truncates-in-place a prior attempt's inode.
        let current_tmp_path = self
            .db_path
            .join(format!("CURRENT.tmp.{}", new_manifest_number));
        if let Err(e) = Self::write_current_file_tmp(&self.db_path, new_manifest_number).ctx() {
            drop(new_writer);
            let _ = fs::remove_file(&new_manifest_path);
            let _ = fs::remove_file(&current_tmp_path);
            tracing::warn!("MANIFEST compaction deferred writing CURRENT: {}", e);
            return;
        }
        if let Err(e) = Self::rename_current_file(&self.db_path, new_manifest_number).ctx() {
            drop(new_writer);
            let _ = fs::remove_file(&new_manifest_path);
            let _ = fs::remove_file(&current_tmp_path);
            tracing::warn!("MANIFEST compaction deferred publishing CURRENT: {}", e);
            return;
        }

        let old_manifest_number = self.manifest_number;
        self.manifest_number = new_manifest_number;
        *self.manifest_writer.lock() = Some(new_writer);
        self.edits_since_snapshot = 0;

        if let Err(e) = Self::fsync_directory(&self.db_path) {
            // The CURRENT rename is visible in the live filesystem (the new
            // writer stays installed for in-process consistency) but its
            // durability is unknown: a crash could recover from either
            // MANIFEST. Both contain every applied edit thanks to the old-
            // writer sync above. Poison further edits so the two files cannot
            // diverge, and keep the old MANIFEST as the fallback.
            self.poisoned.store(true, Ordering::Release);
            tracing::error!(
                "MANIFEST rotation could not fsync the DB directory; \
                 poisoning manifest writer (reopen the database to recover): {}",
                e
            );
            return;
        }

        // Delete old manifest
        let old_manifest_path = self
            .db_path
            .join(format!("MANIFEST-{:06}", old_manifest_number));
        if let Err(e) = fs::remove_file(&old_manifest_path) {
            tracing::warn!(
                "failed to remove old manifest {}: {}",
                old_manifest_path.display(),
                e
            );
        }
    }

    fn set_current_file(db_path: &Path, manifest_number: u64) -> Result<()> {
        Self::write_current_file_tmp(db_path, manifest_number).ctx()?;
        Self::rename_current_file(db_path, manifest_number).ctx()?;
        Self::fsync_directory(db_path).ctx()?;
        Ok(())
    }

    fn write_current_file_tmp(db_path: &Path, manifest_number: u64) -> Result<()> {
        let contents = format!("MANIFEST-{:06}\n", manifest_number);
        // Unique per-attempt name (suffixed with the manifest number being
        // published) so a retry after a failed attempt never reuses the same
        // inode: `fs::File::create` on a fixed name would truncate whatever
        // partially-written file a prior failed attempt left behind.
        let tmp_path = db_path.join(format!("CURRENT.tmp.{}", manifest_number));

        // Write and fsync the temp file before rename to ensure durability
        let file = fs::File::create(&tmp_path).ctx()?;
        let mut writer = BufWriter::new(file);
        writer.write_all(contents.as_bytes()).ctx()?;
        writer.flush().ctx()?;
        writer.get_ref().sync_all().ctx()?;

        Ok(())
    }

    fn rename_current_file(db_path: &Path, manifest_number: u64) -> Result<()> {
        let tmp_path = db_path.join(format!("CURRENT.tmp.{}", manifest_number));
        // Atomic rename
        fs::rename(&tmp_path, db_path.join("CURRENT")).ctx()?;
        Ok(())
    }

    /// Fsync a directory to persist metadata changes (renames, creates).
    fn fsync_directory(dir: &Path) -> Result<()> {
        let f = fs::File::open(dir).ctx()?;
        f.sync_all().ctx()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        // Create
        {
            let vs = VersionSet::create(path, 7).unwrap();
            assert_eq!(vs.current().total_files(), 0);
        }

        // Verify CURRENT file exists
        assert!(path.join("CURRENT").exists());
        let current_content = fs::read_to_string(path.join("CURRENT")).unwrap();
        assert!(current_content.trim().starts_with("MANIFEST-"));

        // Recovery should succeed for a clean (empty) DB
        let vs = VersionSet::recover(path, 7).unwrap();
        assert_eq!(vs.current().total_files(), 0);
    }

    #[test]
    fn test_recovery_rejects_invalid_current_name_without_modifying_other_file() {
        use std::io::Write;

        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("db");
        fs::create_dir(&db_path).unwrap();
        drop(VersionSet::create(&db_path, 7).unwrap());

        let source_manifest = db_path.join("MANIFEST-000001");
        let other_file = root.path().join("other-file");
        let mut other_bytes = fs::read(source_manifest).unwrap();
        other_bytes.extend_from_slice(b"torn");
        let mut file = fs::File::create(&other_file).unwrap();
        file.write_all(&other_bytes).unwrap();
        file.sync_all().unwrap();

        fs::write(db_path.join("CURRENT"), "../other-file\n").unwrap();
        let err = match VersionSet::recover(&db_path, 7) {
            Ok(_) => panic!("recovery should reject an invalid CURRENT name"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("invalid MANIFEST name"),
            "unexpected recovery error: {err}"
        );
        assert_eq!(
            fs::read(&other_file).unwrap(),
            other_bytes,
            "an invalid CURRENT name must not modify an unrelated file"
        );
    }

    #[test]
    fn test_recovery_fails_on_missing_sst() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        // Create and write a MANIFEST edit referencing a non-existent SST
        {
            let vs = VersionSet::create(path, 7).unwrap();
            let mut edit = VersionEdit::new();
            edit.set_last_sequence(100);
            edit.add_file(
                0,
                FileMetaData {
                    number: 10,
                    file_size: 4096,
                    smallest_key: b"aaa".to_vec(),
                    largest_key: b"zzz".to_vec(),
                    has_range_deletions: false,
                },
            );
            let encoded = edit.encode();
            {
                let mut w = vs.manifest_writer.lock();
                if let Some(ref mut writer) = *w {
                    writer.add_record(&encoded).unwrap();
                    writer.sync().unwrap();
                }
            }
        }

        // Recovery must fail because SST file 10 does not exist
        let result = VersionSet::recover(path, 7);
        assert!(result.is_err(), "recovery should fail on missing SST");
    }

    /// A `log_and_apply` whose new SST cannot be opened must reject the
    /// edit wholesale — nothing applied in memory, nothing persisted, no
    /// MANIFEST poisoning — so the caller can retry from a fresh pick.
    /// This is the contract `DB::flush` relies on to treat a failed
    /// post-flush L0 drain as non-fatal.
    #[test]
    fn test_log_and_apply_missing_sst_rejected_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        let mut vs = VersionSet::create(path, 7).unwrap();
        let before = vs.next_file_number();

        let mut edit = VersionEdit::new();
        edit.set_next_file_number(vs.next_file_number());
        edit.add_file(
            1,
            FileMetaData {
                number: 999_999,
                file_size: 4096,
                smallest_key: b"aaa".to_vec(),
                largest_key: b"zzz".to_vec(),
                has_range_deletions: false,
            },
        );
        let err = vs.log_and_apply(edit).unwrap_err();
        assert!(err.to_string().contains("failed to open new SST"));
        assert!(err.to_string().contains("exists_now=false"));

        // Nothing applied, not poisoned, counter intact.
        assert_eq!(vs.current().total_files(), 0);
        assert!(!vs.is_poisoned());
        assert_eq!(vs.next_file_number(), before);

        // A subsequent valid edit still applies, and recovery replays a
        // MANIFEST that never saw the rejected edit.
        let mut ok_edit = VersionEdit::new();
        ok_edit.set_last_sequence(7);
        ok_edit.set_next_file_number(vs.next_file_number());
        vs.log_and_apply(ok_edit).unwrap();
        assert_eq!(vs.last_sequence(), 7);
        drop(vs);

        let recovered = VersionSet::recover(path, 7).unwrap();
        assert_eq!(recovered.current().total_files(), 0);
        assert_eq!(recovered.last_sequence(), 7);
    }

    /// The file-number allocator must never move backwards, even if an edit
    /// carries a stale (lower) `next_file_number` — a regression would
    /// re-issue numbers of files that may still exist on disk.
    #[test]
    fn test_next_file_number_never_regresses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        let mut vs = VersionSet::create(path, 7).unwrap();
        let reserved = vs.reserve_file_numbers(100);
        assert!(vs.next_file_number() >= reserved + 100);

        let mut stale_edit = VersionEdit::new();
        stale_edit.set_next_file_number(reserved); // stale: lower than current
        vs.log_and_apply(stale_edit).unwrap();
        assert_eq!(
            vs.next_file_number(),
            reserved + 100,
            "stale edit must not regress the allocator"
        );
    }

    /// `last_sequence` mirrors the allocator's forward-only rule: a stale
    /// lower value in a live edit or in a replayed MANIFEST record must not
    /// regress the counter, and an over-limit value is rejected on replay.
    #[test]
    fn test_last_sequence_never_regresses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        let mut vs = VersionSet::create(path, 7).unwrap();
        let mut edit = VersionEdit::new();
        edit.set_last_sequence(100);
        vs.log_and_apply(edit).unwrap();
        assert_eq!(vs.last_sequence(), 100);

        // Live apply: stale lower value is ignored.
        let mut stale = VersionEdit::new();
        stale.set_last_sequence(1);
        vs.log_and_apply(stale).unwrap();
        assert_eq!(vs.last_sequence(), 100, "live apply must not regress");
        drop(vs);

        // Replay: the persisted stale record must not regress either.
        let recovered = VersionSet::recover(path, 7).unwrap();
        assert_eq!(recovered.last_sequence(), 100, "replay must not regress");
        drop(recovered);

        // Replay: a sequence above the 56-bit packing limit is corruption.
        let over_limit = {
            let vs = VersionSet::recover(path, 7).unwrap();
            let mut edit = VersionEdit::new();
            edit.set_last_sequence(crate::types::MAX_SEQUENCE_NUMBER + 1);
            let encoded = edit.encode();
            let mut w = vs.manifest_writer.lock();
            if let Some(ref mut writer) = *w {
                writer.add_record(&encoded).unwrap();
                writer.sync().unwrap();
            }
            drop(w);
            VersionSet::recover(path, 7)
        };
        assert!(
            over_limit.is_err(),
            "replay must reject an out-of-range last_sequence"
        );
    }

    #[test]
    fn test_version_edit_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        let mut vs = VersionSet::create(path, 7).unwrap();

        // Allocate file numbers
        let n1 = vs.new_file_number();
        let n2 = vs.new_file_number();
        assert!(n1 < n2);

        let mut edit = VersionEdit::new();
        edit.set_last_sequence(42);
        edit.set_next_file_number(vs.next_file_number());
        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();
        assert_eq!(decoded.last_sequence, Some(42));
    }
}
