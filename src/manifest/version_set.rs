//! VersionSet: manages the MANIFEST file and the chain of Versions.

use std::{
    collections::HashMap,
    fs,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::cache::table_cache::TableCache;
use crate::error::{Error, Result};
use crate::manifest::version::{TableFile, Version};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::sst::table_reader::TableReader;
use crate::types::{SequenceNumber, compare_internal_key};
use crate::wal::{WalReader, WalWriter};
use parking_lot::Mutex;
use ruc::*;

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
    /// Set when the MANIFEST writer is in an unrecoverable state: either a
    /// record append failed mid-write (leaving a torn record in the stream —
    /// appending more records after it would make the file unrecoverable
    /// mid-file), or a rotation published a new MANIFEST whose durability
    /// could not be confirmed. While poisoned, `log_and_apply` fails fast
    /// *before* applying anything, so callers can rely on the invariant
    /// "Err ⇒ edit not applied". Recovery on reopen truncates any torn tail.
    poisoned: bool,
}

impl VersionSet {
    /// Create a new VersionSet (for a fresh database).
    pub fn create(db_path: &Path, num_levels: usize) -> Result<Self> {
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
        let manifest_writer = WalWriter::new(&manifest_path).c(d!())?;

        // Write CURRENT file
        Self::set_current_file(db_path, manifest_number).c(d!())?;

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
            poisoned: false,
        };

        // Write initial snapshot edit
        let mut edit = VersionEdit::new();
        edit.set_next_file_number(vs.next_file_number);
        edit.set_last_sequence(vs.last_sequence);
        vs.log_and_apply(edit).c(d!())?;
        vs.sync_manifest().c(d!())?;

        Ok(vs)
    }

    /// Recover an existing VersionSet from MANIFEST.
    pub fn recover(db_path: &Path, num_levels: usize) -> Result<Self> {
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
            .map_err(|e| eg!(Error::Corruption(format!("cannot read CURRENT: {}", e))))
            .c(d!())?;
        let manifest_name = manifest_name.trim();
        let manifest_path = db_path.join(manifest_name);

        // Parse manifest number from name
        let manifest_number = manifest_name
            .strip_prefix("MANIFEST-")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1);

        // Replay MANIFEST records in two passes:
        // Pass 1: Collect all edits to determine the final file set.
        // Pass 2: Open only the files that survive all deletions.
        // This handles the case where compaction adds + deletes files in
        // separate edits, and the deleted SST no longer exists on disk.
        let mut reader = WalReader::new(&manifest_path).c(d!())?;
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
            let edit = VersionEdit::decode(&data).c(d!())?;
            edits_replayed += 1;

            if let Some(n) = edit.next_file_number {
                next_file_number = n;
            }
            if let Some(n) = edit.log_number {
                log_number = n;
            }
            if let Some(s) = edit.last_sequence {
                last_sequence = s;
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
                        return Err(eg!(Error::Corruption(format!(
                            "MANIFEST deletes SST {} at level {} but it is live at level {}",
                            file_number, level, live_level
                        ))));
                    }
                    None => {
                        return Err(eg!(Error::Corruption(format!(
                            "MANIFEST deletes SST {} at level {} but it is not live",
                            file_number, level
                        ))));
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
                    return Err(eg!(Error::Corruption(format!(
                        "MANIFEST references SST {} at level {} but the database \
                         is configured with num_levels {}; reopen with num_levels >= {}",
                        meta.number,
                        level,
                        num_levels,
                        level + 1
                    ))));
                }
                if let Some((prev_level, _)) = live_files.insert(meta.number, (level, meta.clone()))
                {
                    return Err(eg!(Error::Corruption(format!(
                        "MANIFEST adds SST {} at level {} but it is already live at level {}",
                        meta.number, level, prev_level
                    ))));
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
                    return Err(eg!(Error::Corruption(format!(
                        "cannot open SST {} during recovery: {}",
                        meta.number, e
                    ))));
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
            WalWriter::open_append_truncated(&manifest_path, valid_offset).c(d!())?;

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
            poisoned: false,
        })
    }

    /// Open or create a VersionSet.
    pub fn open(db_path: &Path, num_levels: usize) -> Result<Self> {
        Self::open_with_cache(db_path, num_levels, None)
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
        if self.poisoned {
            return Err(eg!(Error::Corruption(
                "MANIFEST writer poisoned by an earlier write failure; \
                 reopen the database to recover"
                    .to_string()
            )));
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
                return Err(eg!(Error::Corruption(format!(
                    "VersionEdit adds SST {} at out-of-range level {} (num_levels {})",
                    meta.number, level, self.num_levels
                ))));
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
                    return Err(eg!(Error::Corruption(format!(
                        "failed to open new SST {}: {}",
                        meta.number, e
                    ))));
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
                return Err(eg!(Error::Corruption(format!(
                    "VersionEdit deletes SST {} at out-of-range level {} (num_levels {})",
                    file_number, level, self.num_levels
                ))));
            }
            let before = new_version.files[level].len();
            new_version.files[level].retain(|f| f.meta.number != *file_number);
            if new_version.files[level].len() == before {
                return Err(eg!(Error::Corruption(format!(
                    "VersionEdit deletes SST {} at level {} but no such live file exists there",
                    file_number, level
                ))));
            }
        }

        // Before any MANIFEST record referencing the new SST files can become
        // durable — either via a later `sync_manifest()`/`manifest_sync_handle`
        // sync, or via the `maybe_compact_manifest()` snapshot below — fsync the
        // DB directory so the new files' directory entries survive a crash.
        // (The file *contents* are already fsynced by `TableBuilder::finish`.)
        // Otherwise a crash could leave the MANIFEST referencing an SST whose
        // directory entry was lost.
        if !edit.new_files.is_empty() {
            Self::fsync_directory(&self.db_path).c(d!())?;
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
                self.poisoned = true;
                return Err(e).c(d!());
            }
        }

        // Update bookkeeping
        if let Some(n) = edit.next_file_number {
            self.next_file_number = n;
        }
        if let Some(n) = edit.log_number {
            self.log_number = n;
        }
        if let Some(s) = edit.last_sequence {
            self.last_sequence = s;
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
        if let Some(ref mut writer) = *w {
            writer.sync().c(d!())?;
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

    pub fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence
    }

    pub fn set_last_sequence(&mut self, seq: SequenceNumber) {
        self.last_sequence = seq;
    }

    /// Rewrite the MANIFEST with a full snapshot of the current version.
    /// This prevents unbounded MANIFEST growth.
    ///
    /// Called from `log_and_apply` *after* the edit has been applied, so this
    /// must never surface an error. Pre-publish failures are deferred (the
    /// rotation is retried on a later edit). A post-publish directory-fsync
    /// failure poisons the writer instead: at that point both the old and the
    /// new MANIFEST durably contain every applied edit, but a crash could
    /// recover either one — blocking further edits keeps the two from
    /// diverging, and the old MANIFEST is retained as a fallback.
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
        let mut new_writer = match WalWriter::new(&new_manifest_path).c(d!()) {
            Ok(writer) => writer,
            Err(e) => {
                tracing::warn!("MANIFEST compaction deferred creating snapshot: {}", e);
                return;
            }
        };

        // Write the snapshot edit
        let encoded = snapshot_edit.encode();
        if let Err(e) = new_writer.add_record(&encoded).c(d!()) {
            drop(new_writer);
            let _ = fs::remove_file(&new_manifest_path);
            tracing::warn!("MANIFEST compaction deferred writing snapshot: {}", e);
            return;
        }
        if let Err(e) = new_writer.sync().c(d!()) {
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
                tracing::warn!("MANIFEST compaction deferred syncing old manifest: {}", e);
                return;
            }
        }

        // Publish CURRENT in two phases so a post-rename fsync failure cannot
        // leave the live process appending to a different MANIFEST than CURRENT.
        if let Err(e) = Self::write_current_file_tmp(&self.db_path, new_manifest_number).c(d!()) {
            drop(new_writer);
            let _ = fs::remove_file(&new_manifest_path);
            tracing::warn!("MANIFEST compaction deferred writing CURRENT: {}", e);
            return;
        }
        if let Err(e) = Self::rename_current_file(&self.db_path).c(d!()) {
            drop(new_writer);
            let _ = fs::remove_file(&new_manifest_path);
            let _ = fs::remove_file(self.db_path.join("CURRENT.tmp"));
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
            self.poisoned = true;
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
        Self::write_current_file_tmp(db_path, manifest_number).c(d!())?;
        Self::rename_current_file(db_path).c(d!())?;
        Self::fsync_directory(db_path).c(d!())?;
        Ok(())
    }

    fn write_current_file_tmp(db_path: &Path, manifest_number: u64) -> Result<()> {
        let contents = format!("MANIFEST-{:06}\n", manifest_number);
        let tmp_path = db_path.join("CURRENT.tmp");

        // Write and fsync the temp file before rename to ensure durability
        let file = fs::File::create(&tmp_path).c(d!())?;
        let mut writer = BufWriter::new(file);
        writer.write_all(contents.as_bytes()).c(d!())?;
        writer.flush().c(d!())?;
        writer.get_ref().sync_all().c(d!())?;

        Ok(())
    }

    fn rename_current_file(db_path: &Path) -> Result<()> {
        let tmp_path = db_path.join("CURRENT.tmp");
        // Atomic rename
        fs::rename(&tmp_path, db_path.join("CURRENT")).c(d!())?;
        Ok(())
    }

    /// Fsync a directory to persist metadata changes (renames, creates).
    fn fsync_directory(dir: &Path) -> Result<()> {
        let f = fs::File::open(dir).c(d!())?;
        f.sync_all().c(d!())?;
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
