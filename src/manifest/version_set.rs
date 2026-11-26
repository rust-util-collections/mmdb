//! VersionSet: manages the MANIFEST file and the chain of Versions.

use std::{
    collections::{HashMap, HashSet},
    error::Error as StdError,
    fs,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::cache::block_cache::FastBuildHasher;
use crate::cache::table_cache::TableCache;
use crate::error::{Error, Result, ResultExt};
use crate::manifest::file_numbers::FileNumbers;
use crate::manifest::version::{TableFile, Version};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::sst::table_reader::TableReader;
use crate::types::{
    MAX_SEQUENCE_NUMBER, SequenceNumber, compare_internal_key, decode_internal_key, user_key,
};
use crate::wal::{WalReader, WalWriter};
use parking_lot::Mutex;

// Thread-local fault scheduling stays out of production and test-utils builds.
#[cfg(test)]
thread_local! {
    pub(crate) static FAIL_NEXT_SST_DIR_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static BEFORE_MANIFEST_APPEND: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    static BEFORE_SYNC_POISON: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Evidence that all new SST directory entries precede the referencing edit.
#[derive(Debug)]
pub(crate) struct DirSynced(());

pub(crate) fn sync_new_sst_parent_dir(
    handle: &Mutex<Option<fs::File>>,
    poisoned: &AtomicBool,
) -> Result<DirSynced> {
    let dir = handle.lock();
    let dir = dir.as_ref().ok_or_else(Error::read_only)?;
    check_manifest_poison(poisoned)?;
    #[cfg(test)]
    if FAIL_NEXT_SST_DIR_SYNC.with(|fault| fault.replace(false)) {
        if let Some(hook) = BEFORE_SYNC_POISON.with(|h| h.borrow_mut().take()) {
            hook();
        }
        poisoned.store(true, Ordering::Release);
        return Err(std::io::Error::other("injected SST directory sync failure")).ctx();
    }
    if let Err(e) = dir.sync_all() {
        #[cfg(test)]
        if let Some(hook) = BEFORE_SYNC_POISON.with(|h| h.borrow_mut().take()) {
            hook();
        }
        poisoned.store(true, Ordering::Release);
        return Err(e).ctx();
    }
    check_manifest_poison(poisoned)?;
    Ok(DirSynced(()))
}

fn check_manifest_poison(poisoned: &AtomicBool) -> Result<()> {
    // A failed MANIFEST write fail-stops the engine; the on-disk state is
    // recoverable on reopen, so this is not `Corruption`.
    if poisoned.load(Ordering::Acquire) {
        return Err(Error::background(
            "MANIFEST writer poisoned by an earlier write failure; reopen the database to recover",
        ));
    }
    Ok(())
}

/// Confirm durability under the same guard that publishes sync failures.
/// A queued caller must observe poison before attempting another sync, since
/// a later successful fsync cannot recover an earlier failed durability claim.
pub(crate) fn confirm_manifest_durable(
    handle: &Arc<Mutex<Option<WalWriter>>>,
    poisoned: &AtomicBool,
) -> Result<()> {
    let mut writer = handle.lock();
    let writer = writer.as_mut().ok_or_else(Error::read_only)?;
    check_manifest_poison(poisoned)?;
    if let Err(error) = writer.sync() {
        #[cfg(test)]
        if let Some(hook) = BEFORE_SYNC_POISON.with(|h| h.borrow_mut().take()) {
            hook();
        }
        poisoned.store(true, Ordering::Release);
        return Err(error).ctx();
    }
    check_manifest_poison(poisoned)
}

/// First WAL-framed record in every v5 MANIFEST. Fixed-width integers are LE.
#[derive(Clone, Copy)]
struct DBHeader {
    levels: u32,
    prefix_len: u32,
}

impl DBHeader {
    fn new(levels: usize, prefix_len: usize) -> Result<Self> {
        if !(2..=16).contains(&levels) {
            return Err(Error::invalid_argument("num_levels must be in 2..=16"));
        }
        Ok(Self {
            levels: levels as u32,
            prefix_len: u32::try_from(prefix_len)
                .map_err(|_| Error::invalid_argument("prefix_len exceeds u32"))?,
        })
    }

    fn encode(self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[..4].copy_from_slice(b"MMDB");
        bytes[4..8].copy_from_slice(&5u32.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.levels.to_le_bytes());
        bytes[12..].copy_from_slice(&self.prefix_len.to_le_bytes());
        bytes
    }

    fn decode(data: &[u8]) -> Result<Self> {
        if data.len() != 16 || &data[..4] != b"MMDB" || data[4..8] != 5u32.to_le_bytes() {
            return Err(Error::corruption(
                "missing or unsupported MMDB v5 MANIFEST header",
            ));
        }
        let levels = u32::from_le_bytes(data[8..12].try_into().unwrap());
        if !(2..=16).contains(&levels) {
            return Err(Error::corruption("invalid MANIFEST num_levels"));
        }
        Ok(Self {
            levels,
            prefix_len: u32::from_le_bytes(data[12..].try_into().unwrap()),
        })
    }

    fn read(reader: &mut WalReader) -> Result<Self> {
        let data = reader.read_record().ctx()?.unwrap_or_default();
        Self::decode(&data)
    }
}

/// Manages the MANIFEST file and the current Version.
pub struct VersionSet {
    header: DBHeader,
    db_path: PathBuf,
    num_levels: usize,

    /// The current version (immutable snapshot).
    current: Arc<Version>,
    /// File numbers currently live across all levels of `current`.
    /// Maintained incrementally in `log_and_apply_synced` after MANIFEST append
    /// succeeds so duplicate checks are O(|edit|) instead of O(|total live SSTs|).
    live_file_numbers: HashSet<u64, FastBuildHasher>,

    /// Next file number to allocate.
    pub(crate) file_numbers: Arc<FileNumbers>,
    /// Current WAL log number.
    log_number: u64,
    /// Last sequence number used.
    last_sequence: SequenceNumber,
    /// Current MANIFEST file number.
    manifest_number: u64,
    /// MANIFEST writer, behind its own lock so `sync` can be called
    /// without holding the main DB mutex.
    manifest_writer: Arc<Mutex<Option<WalWriter>>>,
    /// Open store-directory handle shared across unlocked SST and CURRENT
    /// directory syncs so `sync_all()` and `poisoned` publication serialize.
    parent_dir: Arc<Mutex<Option<fs::File>>>,
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

    fn open_table(
        db_path: &Path,
        cache: Option<&TableCache>,
        meta: &FileMetaData,
    ) -> Result<TableFile> {
        let reader = if let Some(cache) = cache {
            cache.get_reader(meta.number)
        } else {
            TableReader::open(&db_path.join(format!("{:06}.sst", meta.number))).map(Arc::new)
        }
        .map_err(|e| {
            let path = db_path.join(format!("{:06}.sst", meta.number));
            let note = format!("failed to open referenced SST {}", path.display());
            if e.source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .is_some_and(|cause| cause.kind() == io::ErrorKind::NotFound)
            {
                Error::corruption(note).with_source(e)
            } else {
                e.context(note)
            }
        })?;
        if reader.file_size() != meta.file_size {
            return Err(Error::corruption(format!(
                "MANIFEST records {} bytes for SST {}, file contains {} bytes",
                meta.file_size,
                meta.number,
                reader.file_size()
            )));
        }
        if reader.has_range_deletions() != meta.has_range_deletions {
            return Err(Error::corruption(format!(
                "MANIFEST range-deletion flag disagrees with SST {}",
                meta.number
            )));
        }
        if !reader.has_data_partitions() && !reader.has_range_deletions() {
            return Err(Error::corruption(format!(
                "SST {} has neither data blocks nor range deletions",
                meta.number
            )));
        }
        decode_internal_key(&meta.smallest_key)
            .with_ctx(|| format!("invalid smallest_key on SST {}", meta.number))?;
        decode_internal_key(&meta.largest_key)
            .with_ctx(|| format!("invalid largest_key on SST {}", meta.number))?;
        if compare_internal_key(&meta.smallest_key, &meta.largest_key).is_gt() {
            return Err(Error::corruption(format!(
                "SST {} has inverted key bounds",
                meta.number
            )));
        }
        Ok(TableFile {
            meta: meta.clone(),
            reader,
        })
    }

    /// Validate the L1+ invariant that point-lookup binary search relies on:
    /// within each level >= 1, files sorted by `smallest_key` must have
    /// pairwise-disjoint user-key ranges. Compaction cuts output files at
    /// user-key boundaries, so two live L1+ files sharing a user key can only
    /// come from a malformed MANIFEST or an internal logic bug — installing
    /// them would make binary-search reads silently miss live keys.
    fn validate_level_disjointness(version: &Version) -> Result<()> {
        for level in 1..version.num_levels {
            let files = version.level_files(level);
            for file in files {
                decode_internal_key(&file.meta.smallest_key)?;
                decode_internal_key(&file.meta.largest_key)?;
                if compare_internal_key(&file.meta.smallest_key, &file.meta.largest_key).is_gt() {
                    return Err(Error::corruption(format!(
                        "SST {} has inverted key bounds at level {}",
                        file.meta.number, level
                    )));
                }
            }
            for pair in files.windows(2) {
                let prev = &pair[0].meta;
                let next = &pair[1].meta;
                if user_key(&prev.largest_key) >= user_key(&next.smallest_key) {
                    return Err(Error::corruption(format!(
                        "SST {} and SST {} have overlapping key ranges at level {}",
                        prev.number, next.number, level
                    )));
                }
            }
        }
        Ok(())
    }

    /// Create a new VersionSet (for a fresh database). Test-only wrapper.
    #[cfg(test)]
    pub(crate) fn create(db_path: &Path, num_levels: usize) -> Result<Self> {
        Self::create_with_cache(db_path, num_levels, 0, None)
    }

    fn validate_uncommitted_initial_manifest(path: &Path) -> Result<()> {
        let mut reader = WalReader::new(path).ctx()?;
        match reader.read_record() {
            Ok(None) => return Ok(()),
            Ok(Some(data)) => {
                DBHeader::decode(&data)?;
            }
            Err(_)
                if reader.last_error_is_truncation()
                    && reader.rest_is_zero_padding().unwrap_or(false) =>
            {
                return Ok(());
            }
            Err(e) => return Err(e),
        }
        let mut edits = 0usize;
        loop {
            match reader.read_record() {
                Ok(None) => return Ok(()),
                Ok(Some(data)) => {
                    edits += 1;
                    let edit = VersionEdit::decode(&data).ctx()?;
                    if edits > 1
                        || edit.log_number.is_some()
                        || edit.last_sequence.unwrap_or(0) != 0
                        || !edit.new_files.is_empty()
                        || !edit.deleted_files.is_empty()
                    {
                        return Err(Error::corruption(
                            "CURRENT is missing alongside persisted MANIFEST state",
                        ));
                    }
                }
                Err(_)
                    if reader.last_error_is_truncation()
                        && reader.rest_is_zero_padding().unwrap_or(false) =>
                {
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn ensure_creatable_store_dir(db_path: &Path) -> Result<()> {
        for entry in fs::read_dir(db_path).with_ctx(|| "reading DB directory")? {
            let entry = entry.with_ctx(|| "reading DB directory entry")?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".sst") {
                return Err(Error::corruption(
                    "CURRENT is missing alongside existing SST files",
                ));
            }
            if name.ends_with(".wal") {
                return Err(Error::corruption(
                    "CURRENT is missing alongside existing WAL files",
                ));
            }
            if let Some(rest) = name.strip_prefix("MANIFEST-")
                && rest.parse::<u64>().is_ok()
            {
                if rest != "000001" {
                    return Err(Error::corruption(format!(
                        "CURRENT is missing alongside existing {name}"
                    )));
                }
                Self::validate_uncommitted_initial_manifest(&entry.path()).ctx()?;
            } else if let Some(rest) = name.strip_prefix("CURRENT.tmp.")
                && rest.parse::<u64>().is_ok_and(|n| n != 1)
            {
                return Err(Error::corruption(format!(
                    "CURRENT is missing alongside rotated {name}"
                )));
            }
        }
        Ok(())
    }

    /// Create a new database store's VersionSet, rejecting directories that
    /// already hold persisted store files without `CURRENT`.
    pub fn create_db_with_cache(
        db_path: &Path,
        num_levels: usize,
        prefix_len: usize,
        table_cache: Option<Arc<TableCache>>,
    ) -> Result<Self> {
        DBHeader::new(num_levels, prefix_len)?;
        Self::ensure_creatable_store_dir(db_path)?;
        Self::create_with_cache(db_path, num_levels, prefix_len, table_cache)
    }

    /// Create a new VersionSet with an optional table cache (internal / unit-test helper).
    pub(crate) fn create_with_cache(
        db_path: &Path,
        num_levels: usize,
        prefix_len: usize,
        table_cache: Option<Arc<TableCache>>,
    ) -> Result<Self> {
        let header = DBHeader::new(num_levels, prefix_len)?;
        let current = Arc::new(Version::new(num_levels));
        let manifest_number = 1;
        let manifest_path = db_path.join(format!("MANIFEST-{:06}", manifest_number));
        let mut manifest_writer = WalWriter::new(&manifest_path).ctx()?;
        manifest_writer.add_record(&header.encode()).ctx()?;
        let parent_dir = fs::File::open(db_path).ctx()?;

        let mut vs = Self {
            header,
            db_path: db_path.to_path_buf(),
            num_levels,
            current,
            live_file_numbers: HashSet::with_hasher(FastBuildHasher),
            file_numbers: Arc::new(FileNumbers::new(2)), // 1 is used by MANIFEST
            log_number: 0,
            last_sequence: 0,
            manifest_number,
            manifest_writer: Arc::new(Mutex::new(Some(manifest_writer))),
            parent_dir: Arc::new(Mutex::new(Some(parent_dir))),
            table_cache,
            edits_since_snapshot: 0,
            poisoned: Arc::new(AtomicBool::new(false)),
        };

        // Write initial snapshot edit
        let mut edit = VersionEdit::new();
        edit.set_next_file_number(vs.next_file_number());
        edit.set_last_sequence(vs.last_sequence);
        vs.apply_metadata(edit).ctx()?;
        vs.sync_manifest().ctx()?;
        Self::write_current_file_tmp(db_path, manifest_number).ctx()?;
        Self::rename_current_file(db_path, manifest_number).ctx()?;
        vs.parent_dir
            .lock()
            .as_ref()
            .expect("writable parent_dir")
            .sync_all()
            .ctx()?;

        Ok(vs)
    }

    /// Test-only wrapper for [`Self::recover_with_cache`].
    #[cfg(test)]
    pub(crate) fn recover(db_path: &Path, num_levels: usize) -> Result<Self> {
        Self::recover_with_cache(db_path, num_levels, 0, None)
    }

    /// Recover with an optional table cache (unit-test helper without DB WAL floor).
    #[cfg(test)]
    pub fn recover_with_cache(
        db_path: &Path,
        num_levels: usize,
        prefix_len: usize,
        table_cache: Option<Arc<TableCache>>,
    ) -> Result<Self> {
        Self::recover_with_cache_mode(
            db_path,
            Some(DBHeader::new(num_levels, prefix_len)?),
            table_cache,
            false,
        )
    }

    /// Recover a database store's VersionSet, enforcing DB-level WAL floor invariants.
    pub fn recover_db_with_cache(
        db_path: &Path,
        num_levels: usize,
        prefix_len: usize,
        table_cache: Option<Arc<TableCache>>,
    ) -> Result<Self> {
        Self::recover_with_cache_mode(
            db_path,
            Some(DBHeader::new(num_levels, prefix_len)?),
            table_cache,
            true,
        )
    }

    /// Recover an existing VersionSet without opening or repairing its
    /// MANIFEST for append (unit-test helper without DB WAL floor).
    #[cfg(test)]
    pub fn recover_read_only_with_cache(
        db_path: &Path,
        table_cache: Option<Arc<TableCache>>,
    ) -> Result<Self> {
        Self::recover_with_cache_mode(db_path, None, table_cache, false)
    }

    /// Recover an existing database store's VersionSet read-only, enforcing DB-level WAL floor invariants.
    pub fn recover_db_read_only_with_cache(
        db_path: &Path,
        table_cache: Option<Arc<TableCache>>,
    ) -> Result<Self> {
        Self::recover_with_cache_mode(db_path, None, table_cache, true)
    }

    fn recover_with_cache_mode(
        db_path: &Path,
        requested: Option<DBHeader>,
        table_cache: Option<Arc<TableCache>>,
        enforce_db_wal_floor: bool,
    ) -> Result<Self> {
        // Read CURRENT to find manifest file
        let current_path = db_path.join("CURRENT");
        let contents = fs::read(&current_path).with_ctx(|| "reading CURRENT")?;
        let manifest_name = std::str::from_utf8(&contents)
            .map_err(|e| Error::corruption("CURRENT contains invalid UTF-8").with_source(e))?;
        let manifest_name = manifest_name.trim_end_matches(['\r', '\n']);
        let manifest_number = Self::parse_manifest_number(manifest_name).ctx()?;
        let manifest_path = db_path.join(format!("MANIFEST-{manifest_number:06}"));

        // Replay MANIFEST records in two passes:
        // Pass 1: Collect all edits to determine the final file set.
        // Pass 2: Open only the files that survive all deletions.
        // This handles the case where compaction adds + deletes files in
        // separate edits, and the deleted SST no longer exists on disk.
        let mut reader = WalReader::new(&manifest_path).ctx()?;
        let header = DBHeader::read(&mut reader).ctx()?;
        if requested.is_some_and(|expected| {
            header.levels != expected.levels || header.prefix_len != expected.prefix_len
        }) {
            return Err(Error::invalid_argument(format!(
                "MANIFEST geometry requires num_levels={} and prefix_len={}",
                header.levels, header.prefix_len
            )));
        }
        let num_levels = header.levels as usize;
        let mut next_file_number = manifest_number
            .checked_add(1)
            .ok_or_else(|| Error::corruption("MANIFEST file number overflows allocator"))?
            .max(2);
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
                // Tolerate only structural short-reads when nothing but zero
                // padding follows (torn tail); `open_append_truncated` below
                // then discards the tail. Checksum/type failure after a full
                // untrusted length read can already have skipped later edits —
                // fail the open instead of treating a zero suffix as proof.
                Err(e)
                    if reader.last_error_is_truncation()
                        && reader.rest_is_zero_padding().unwrap_or(false) =>
                {
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
                if n < log_number {
                    return Err(Error::corruption(format!(
                        "MANIFEST log_number regressed from {} to {}",
                        log_number, n
                    )));
                }
                let after_log = n
                    .checked_add(1)
                    .ok_or_else(|| Error::corruption("MANIFEST log_number overflows allocator"))?;
                next_file_number = next_file_number.max(after_log);
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
                        "MANIFEST references SST {} at level {level} outside its {num_levels} levels",
                        meta.number
                    )));
                }
                let after_sst = meta.number.checked_add(1).ok_or_else(|| {
                    Error::corruption("MANIFEST SST file number overflows allocator")
                })?;
                next_file_number = next_file_number.max(after_sst);
                if let Some((prev_level, _)) = live_files.insert(meta.number, (level, meta.clone()))
                {
                    return Err(Error::corruption(format!(
                        "MANIFEST adds SST {} at level {} but it is already live at level {}",
                        meta.number, level, prev_level
                    )));
                }
            }
        }

        if edits_replayed == 0 {
            return Err(Error::corruption("MANIFEST contains no version edits"));
        }

        // Pass 2: Open only live files
        let mut version = Version::new(num_levels);
        for (level, meta) in live_files.values() {
            let file = Self::open_table(db_path, table_cache.as_deref(), meta).ctx()?;
            let files = Arc::make_mut(&mut version.files[*level]);
            files.insert(files.len(), file);
        }

        version.sort_recovered_files();
        // The replayed record stream passed per-record checksums, but the
        // recovered file set must still satisfy the read-path invariant.
        Self::validate_level_disjointness(&version).ctx()?;

        if enforce_db_wal_floor && log_number == 0 {
            if !live_files.is_empty() || last_sequence != 0 {
                return Err(Error::corruption(
                    "MANIFEST has zero log_number alongside persisted DB state",
                ));
            }
            for entry in fs::read_dir(db_path).with_ctx(|| "reading DB directory")? {
                let entry = entry.with_ctx(|| "reading DB directory entry")?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(".sst") {
                    return Err(Error::corruption(
                        "MANIFEST has zero log_number alongside existing SST files",
                    ));
                }
                if name.ends_with(".wal") && entry.metadata().with_ctx(|| "stat WAL")?.len() > 0 {
                    return Err(Error::corruption(
                        "MANIFEST has zero log_number alongside non-empty WAL",
                    ));
                }
            }
        }

        // Writable recovery repairs a torn tail and reopens the MANIFEST for
        // append. Read-only recovery must leave the file byte-for-byte intact.
        let (manifest_writer, parent_dir) = if requested.is_none() {
            (None, None)
        } else {
            let valid_offset = reader.last_valid_offset();
            let writer = WalWriter::open_append_truncated(&manifest_path, valid_offset).ctx()?;
            let dir_file = fs::File::open(db_path).ctx()?;
            (Some(writer), Some(dir_file))
        };

        let live_file_numbers: HashSet<u64, FastBuildHasher> = live_files.keys().copied().collect();

        Ok(Self {
            header,
            db_path: db_path.to_path_buf(),
            num_levels,
            current: Arc::new(version),
            live_file_numbers,
            file_numbers: Arc::new(FileNumbers::new(next_file_number)),
            log_number,
            last_sequence,
            manifest_number,
            manifest_writer: Arc::new(Mutex::new(manifest_writer)),
            parent_dir: Arc::new(Mutex::new(parent_dir)),
            table_cache,
            edits_since_snapshot: edits_replayed,
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Append a metadata-only edit. New SST references require a directory sync token.
    pub fn apply_metadata(&mut self, edit: VersionEdit) -> Result<()> {
        if !edit.new_files.is_empty() {
            return Err(Error::invalid_argument(
                "metadata-only edit contains new SSTs",
            ));
        }
        self.log_and_apply_synced(edit, DirSynced(()))
    }

    #[cfg(test)]
    pub fn log_and_apply(&mut self, edit: VersionEdit) -> Result<()> {
        if self.manifest_writer.lock().is_none() {
            return Err(Error::read_only());
        }
        if edit.new_files.is_empty() {
            return self.apply_metadata(edit);
        }
        let synced = sync_new_sst_parent_dir(&self.parent_dir, &self.poisoned)?;
        self.log_and_apply_synced(edit, synced)
    }

    /// Publish an edit after syncing its new SST directory entries. On error the
    /// edit was rejected before publication, so its newly built SSTs can be removed.
    pub fn log_and_apply_synced(&mut self, edit: VersionEdit, _synced: DirSynced) -> Result<()> {
        if self.manifest_writer.lock().is_none() {
            return Err(Error::read_only());
        }
        check_manifest_poison(&self.poisoned)?;
        if edit.log_number == Some(u64::MAX) {
            return Err(Error::corruption(
                "VersionEdit log_number overflows allocator",
            ));
        }

        // Build new version from current + edit FIRST, before persisting.
        // This ensures that if an SST fails to open, the MANIFEST is not
        // polluted with an edit referencing a broken file.
        let mut new_version = (*self.current).clone();

        // Deletions are applied before additions, matching recovery replay.
        // That order defines the semantics when an edit both deletes and adds
        // the same file number (the trivial-move shape).
        for (level, file_number) in &edit.deleted_files {
            let level = *level as usize;
            if level >= self.num_levels {
                return Err(Error::corruption(format!(
                    "VersionEdit deletes SST {} at out-of-range level {} (num_levels {})",
                    file_number, level, self.num_levels
                )));
            }
            if !Arc::make_mut(&mut new_version.files[level]).remove_file(*file_number) {
                return Err(Error::corruption(format!(
                    "VersionEdit deletes SST {} at level {} but no such live file exists there",
                    file_number, level
                )));
            }
        }

        // Recovery rejects an addition whose number is already live at any
        // level, including a duplicate within the same edit. Check against the
        // persistent `live_file_numbers` set (accounting for `deleted_files`
        // removed earlier in this edit, such as trivial moves, and duplicates
        // within `edit.new_files`) without scanning untouched levels or
        // mutating `self.live_file_numbers` before MANIFEST append succeeds.
        let mut removed_in_edit: smallvec::SmallVec<[u64; 16]> = smallvec::SmallVec::new();
        if !edit.new_files.is_empty() && !edit.deleted_files.is_empty() {
            removed_in_edit.extend(edit.deleted_files.iter().map(|&(_, n)| n));
            removed_in_edit.sort_unstable();
        }
        let mut added_inline: smallvec::SmallVec<[u64; 16]> = smallvec::SmallVec::new();
        let mut added_overflow: Option<HashSet<u64, FastBuildHasher>> = (edit.new_files.len() > 16)
            .then(|| HashSet::with_capacity_and_hasher(edit.new_files.len(), FastBuildHasher));

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
            if meta.number == u64::MAX {
                return Err(Error::corruption(
                    "VersionEdit SST number overflows allocator",
                ));
            }
            let still_live = self.live_file_numbers.contains(&meta.number)
                && removed_in_edit.binary_search(&meta.number).is_err();
            let dup_in_edit = if let Some(ref mut set) = added_overflow {
                !set.insert(meta.number)
            } else if added_inline.contains(&meta.number) {
                true
            } else {
                added_inline.push(meta.number);
                false
            };
            if still_live || dup_in_edit {
                return Err(Error::corruption(format!(
                    "VersionEdit adds SST {} at level {} but it is already live",
                    meta.number, level
                )));
            }
            let file = Self::open_table(&self.db_path, self.table_cache.as_deref(), meta).ctx()?;
            let files = Arc::make_mut(&mut new_version.files[level]);
            let index = if level == 0 {
                0
            } else {
                let idx = files.partition_point(|other| {
                    compare_internal_key(&other.meta.smallest_key, &meta.smallest_key).is_lt()
                });
                // `self.current` already passed `validate_level_disjointness`,
                // deletions preserve strict ordering, and `open_table` verified
                // `meta.smallest_key <= meta.largest_key`. Checking the two
                // insertion neighbors preserves pairwise disjointness across the
                // entire level without scanning untouched files or levels.
                if idx > 0 {
                    let prev = &files[idx - 1].meta;
                    if user_key(&prev.largest_key) >= user_key(&meta.smallest_key) {
                        return Err(Error::corruption(format!(
                            "SST {} and SST {} have overlapping key ranges at level {}",
                            prev.number, meta.number, level
                        )));
                    }
                }
                if idx < files.len() {
                    let next = &files[idx].meta;
                    if user_key(&meta.largest_key) >= user_key(&next.smallest_key) {
                        return Err(Error::corruption(format!(
                            "SST {} and SST {} have overlapping key ranges at level {}",
                            meta.number, next.number, level
                        )));
                    }
                }
                idx
            };
            files.insert(index, file);
        }

        // Version built successfully — now persist to MANIFEST (write only, no sync).
        // Callers on hot paths should release the main lock and call
        // `sync_manifest()` separately to avoid stalling other operations.
        let encoded = edit.encode();
        #[cfg(test)]
        if let Some(hook) = BEFORE_MANIFEST_APPEND.with(|h| h.borrow_mut().take()) {
            hook();
        }
        {
            let mut w = self.manifest_writer.lock();
            check_manifest_poison(&self.poisoned)?;
            if let Some(n) = edit.log_number
                && (n == u64::MAX || n < self.log_number)
            {
                return Err(Error::corruption(
                    "VersionEdit log_number overflows allocator or regressed",
                ));
            }
            if let Some(ref mut writer) = *w
                && let Err(e) = writer.add_record(&encoded)
            {
                // The failed append may have left a torn record in the
                // MANIFEST stream. Appending further records after it would
                // produce mid-file corruption that recovery rejects (only
                // *tail* corruption is tolerated). Poison the writer so no
                // more records can follow; recovery truncates the torn tail.
                self.poisoned.store(true, Ordering::Release);
                drop(w);
                return Err(e).ctx();
            }
        }

        // Update bookkeeping. Forward-only: file numbers are never reused,
        // so an edit carrying a stale (lower) counter value must never move
        // the allocator backwards — a regression would re-issue numbers of
        // files that may still exist on disk or be referenced by the version.
        if let Some(n) = edit.next_file_number {
            self.file_numbers.advance_to(n);
        }
        if let Some(n) = edit.log_number {
            self.file_numbers.advance_to(n + 1);
            self.log_number = n;
        }
        for (_, meta) in &edit.new_files {
            self.file_numbers.advance_to(meta.number + 1);
        }
        // Forward-only for the same reason: a stale lower sequence would let
        // newly-assigned sequence numbers collide with already-committed
        // entries and lose MVCC precedence to them.
        if let Some(s) = edit.last_sequence {
            self.last_sequence = self.last_sequence.max(s);
        }

        for &(_, file_number) in &edit.deleted_files {
            self.live_file_numbers.remove(&file_number);
        }
        for (_, meta) in &edit.new_files {
            self.live_file_numbers.insert(meta.number);
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
    /// The SST builder or recovery path syncs the parent directory before
    /// publication. This call syncs the MANIFEST containing those references.
    pub fn sync_manifest(&self) -> Result<()> {
        confirm_manifest_durable(&self.manifest_writer, &self.poisoned)
    }

    /// Return a handle for syncing the MANIFEST outside the main DB lock.
    /// Clone the Arc, release the DB lock, then call `lock() → sync()`.
    pub fn manifest_sync_handle(&self) -> Arc<Mutex<Option<WalWriter>>> {
        Arc::clone(&self.manifest_writer)
    }

    /// Return the persistent store-directory handle for unlocked SST directory syncs.
    pub fn dir_sync_handle(&self) -> Arc<Mutex<Option<fs::File>>> {
        Arc::clone(&self.parent_dir)
    }

    /// Release this handle's readers and writer after the last worker exits.
    pub fn release_files(&mut self) {
        self.manifest_writer.lock().take();
        self.parent_dir.lock().take();
        self.live_file_numbers.clear();
        self.current = Arc::new(Version::new(self.num_levels));
    }

    /// Get the current version.
    pub fn current(&self) -> Arc<Version> {
        self.current.clone()
    }

    pub fn geometry(&self) -> (usize, usize) {
        (self.num_levels, self.header.prefix_len as usize)
    }

    pub fn next_file_number(&self) -> u64 {
        self.file_numbers.next()
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
    #[cfg(test)]
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
        let Ok(new_manifest_number) = self.file_numbers.allocate() else {
            tracing::warn!("MANIFEST rotation deferred: database file numbers exhausted");
            return;
        };

        let snapshot_edit = VersionEdit::from_version_snapshot(
            &self.current,
            self.log_number,
            self.next_file_number(),
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
        if let Err(e) = new_writer
            .add_record(&self.header.encode())
            .and_then(|()| new_writer.add_record(&encoded))
            .ctx()
        {
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
        if let Err(e) = self.sync_manifest() {
            drop(new_writer);
            let _ = fs::remove_file(&new_manifest_path);
            tracing::error!(
                "MANIFEST compaction could not sync the old manifest; \
                 reopen the database to recover: {}",
                e
            );
            return;
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

        {
            let dir = self.parent_dir.lock();
            let sync_res = dir
                .as_ref()
                .ok_or_else(Error::read_only)
                .and_then(|f| f.sync_all().ctx());
            if let Err(e) = sync_res {
                // The CURRENT rename is visible in the live filesystem (the new
                // writer stays installed for in-process consistency) but its
                // durability is unknown: a crash could recover from either
                // MANIFEST. Both contain every applied edit thanks to the old-
                // writer sync above. Poison further edits while holding both
                // sync guards so concurrent directory/manifest syncs observe
                // poison before proceeding, and keep the old MANIFEST.
                let _writer = self.manifest_writer.lock();
                self.poisoned.store(true, Ordering::Release);
                tracing::error!(
                    "MANIFEST rotation could not fsync the DB directory; \
                     poisoning manifest writer (reopen the database to recover): {}",
                    e
                );
                return;
            }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn install_shares_untouched_levels_and_preserves_pinned_versions() {
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let build = |number, user_key: &[u8]| {
            let mut builder = TableBuilder::new(
                &dir.path().join(format!("{number:06}.sst")),
                TableBuildOptions::default(),
            )
            .unwrap();
            builder
                .add(
                    InternalKey::new(user_key, 1, ValueType::Value).as_bytes(),
                    b"v",
                )
                .unwrap();
            let result = builder.finish().unwrap();
            FileMetaData {
                number,
                file_size: result.file_size,
                smallest_key: result.smallest_key.unwrap(),
                largest_key: result.largest_key.unwrap(),
                has_range_deletions: false,
            }
        };
        let long_key = vec![b'z'; 64 * 1024];
        let large = build(10, &long_key);
        let small = build(11, b"a");
        let mut vs = VersionSet::create(dir.path(), 7).unwrap();
        let mut edit = VersionEdit::new();
        edit.set_next_file_number(12);
        edit.add_file(2, large);
        vs.log_and_apply(edit).unwrap();
        let before = vs.current();

        let mut edit = VersionEdit::new();
        edit.add_file(0, small.clone());
        let (_, allocated) = crate::test_alloc::count_allocated(|| vs.log_and_apply(edit).unwrap());
        eprintln!(
            "small Version install with 128KiB untouched endpoints allocated {allocated} bytes"
        );
        assert!(
            allocated < 64 * 1024,
            "untouched endpoints were copied: {allocated}"
        );
        let after = vs.current();
        assert!(Arc::ptr_eq(&before.files[2], &after.files[2]));
        assert!(before.level_files(0).is_empty());
        assert_eq!(after.level_files(0)[0].meta.number, 11);

        let mut edit = VersionEdit::new();
        edit.delete_file(0, 11);
        edit.add_file(1, small);
        vs.log_and_apply(edit).unwrap();
        assert_eq!(after.level_files(0)[0].meta.number, 11);
        assert!(after.level_files(1).is_empty());
        assert_eq!(vs.current().level_files(1)[0].meta.number, 11);
        vs.sync_manifest().unwrap();
        drop(vs);
        let recovered = VersionSet::recover(dir.path(), 7).unwrap();
        assert!(recovered.current().level_files(0).is_empty());
        assert_eq!(recovered.current().level_files(1)[0].meta.number, 11);
        assert_eq!(recovered.current().level_files(2)[0].meta.number, 10);
    }

    #[test]
    fn manifest_sync_failure_blocks_following_confirmation() {
        use crate::wal::writer::FAIL_NEXT_SYNC;
        use std::{sync::mpsc, thread, time::Duration};

        let dir = tempfile::tempdir().unwrap();
        let vs = VersionSet::create(dir.path(), 7).unwrap();
        let handle = vs.manifest_sync_handle();
        let poison = vs.poison_flag();
        let (start_tx, start_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        thread::scope(|scope| {
            let next_handle = handle.clone();
            let next_poison = poison.clone();
            scope.spawn(move || {
                start_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                ready_tx.send(()).unwrap();
                result_tx
                    .send(confirm_manifest_durable(&next_handle, &next_poison))
                    .unwrap();
            });
            let checked_handle = handle.clone();
            BEFORE_SYNC_POISON.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    start_tx.send(()).unwrap();
                    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    assert!(
                        checked_handle.try_lock().is_none(),
                        "the failed sync must retain the writer until poison is published"
                    );
                }));
            });
            FAIL_NEXT_SYNC.with(|fail| fail.set(true));
            assert_eq!(
                confirm_manifest_durable(&handle, &poison)
                    .unwrap_err()
                    .kind(),
                ErrorKind::Io
            );
            assert_eq!(
                result_rx
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap()
                    .unwrap_err()
                    .kind(),
                ErrorKind::Background
            );
        });
    }

    #[test]
    fn manifest_append_rechecks_concurrent_sync_failure() {
        use crate::wal::writer::FAIL_NEXT_SYNC;

        let dir = tempfile::tempdir().unwrap();
        let mut vs = VersionSet::create(dir.path(), 7).unwrap();
        let path = dir.path().join("MANIFEST-000001");
        let before = fs::read(&path).unwrap();
        let handle = vs.manifest_sync_handle();
        let poison = vs.poison_flag();
        BEFORE_MANIFEST_APPEND.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                FAIL_NEXT_SYNC.with(|fail| fail.set(true));
                assert!(confirm_manifest_durable(&handle, &poison).is_err());
            }));
        });
        let mut edit = VersionEdit::new();
        edit.set_last_sequence(7);
        assert_eq!(
            vs.log_and_apply(edit).unwrap_err().kind(),
            ErrorKind::Background
        );
        assert_eq!(vs.last_sequence(), 0);
        drop(vs);
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[test]
    fn torn_manifest_payload_keeps_prefix_and_accepts_new_edits() {
        for zero_extended in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut vs = VersionSet::create(dir.path(), 7).unwrap();
            for seq in [1, 0x0001_0203_0405_0607] {
                let mut edit = VersionEdit::new();
                edit.set_last_sequence(seq);
                vs.log_and_apply(edit).unwrap();
            }
            vs.sync_manifest().unwrap();
            drop(vs);
            let path = dir.path().join("MANIFEST-000001");
            let mut bytes = fs::read(&path).unwrap();
            let tail = bytes.len() - 4;
            if zero_extended {
                bytes[tail..].fill(0);
            } else {
                bytes.truncate(tail);
            }
            fs::write(&path, bytes).unwrap();
            let mut recovered = VersionSet::recover(dir.path(), 7).unwrap();
            assert_eq!(recovered.last_sequence(), 1);
            let mut edit = VersionEdit::new();
            edit.set_last_sequence(2);
            recovered.log_and_apply(edit).unwrap();
            recovered.sync_manifest().unwrap();
            drop(recovered);
            assert_eq!(
                VersionSet::recover(dir.path(), 7).unwrap().last_sequence(),
                2
            );
        }
    }

    #[test]
    fn changed_synced_edit_header_preserves_manifest_on_failed_recovery() {
        for (replacement, length_delta) in [
            (Some(2), 0),
            (Some(3), 0),
            (Some(4), 0),
            (None, -1isize),
            (None, 1),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut vs = VersionSet::create(dir.path(), 7).unwrap();
            let mut edit = VersionEdit::new();
            edit.set_last_sequence(7);
            vs.log_and_apply(edit).unwrap();
            vs.sync_manifest().unwrap();
            drop(vs);
            let path = dir.path().join("MANIFEST-000001");
            let mut bytes = fs::read(&path).unwrap();
            let offset = 7 + u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
            assert_eq!(bytes[offset + 6], 1);
            assert_eq!(bytes.last(), Some(&0));
            if let Some(replacement) = replacement {
                bytes[offset + 6] = replacement;
            } else {
                let length = u16::from_le_bytes([bytes[offset + 4], bytes[offset + 5]]) as usize;
                let changed = length.checked_add_signed(length_delta).unwrap() as u16;
                bytes[offset + 4..offset + 6].copy_from_slice(&changed.to_le_bytes());
            }
            fs::write(&path, &bytes).unwrap();
            assert!(VersionSet::recover_read_only_with_cache(dir.path(), None).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
            assert!(VersionSet::recover(dir.path(), 7).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn enlarged_payload_preserves_later_manifest_edits() {
        for adjustment in [-1isize, 0, 17] {
            let dir = tempfile::tempdir().unwrap();
            let mut vs = VersionSet::create(dir.path(), 7).unwrap();
            for seq in [1, 2] {
                let mut edit = VersionEdit::new();
                edit.set_last_sequence(seq);
                vs.log_and_apply(edit).unwrap();
            }
            vs.sync_manifest().unwrap();
            drop(vs);
            let path = dir.path().join("MANIFEST-000001");
            let mut bytes = fs::read(&path).unwrap();
            let middle = 7 + u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
            let enlarged = (bytes.len() - middle - 7)
                .checked_add_signed(adjustment)
                .unwrap();
            bytes[middle + 4..middle + 6].copy_from_slice(&(enlarged as u16).to_le_bytes());
            fs::write(&path, &bytes).unwrap();
            assert!(VersionSet::recover_read_only_with_cache(dir.path(), None).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
            assert!(VersionSet::recover(dir.path(), 7).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn read_only_recovery_leaves_manifest_unchanged_and_rejects_writes() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        drop(VersionSet::create(path, 7).unwrap());

        let manifest_path = path.join("MANIFEST-000001");
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&manifest_path)
            .unwrap();
        file.write_all(&[0]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let before = fs::read(&manifest_path).unwrap();

        let mut vs = VersionSet::recover_read_only_with_cache(path, None).unwrap();
        assert_eq!(fs::read(&manifest_path).unwrap(), before);
        assert_eq!(vs.sync_manifest().unwrap_err().kind(), ErrorKind::ReadOnly);
        assert_eq!(
            vs.log_and_apply(VersionEdit::new()).unwrap_err().kind(),
            ErrorKind::ReadOnly
        );
        assert_eq!(fs::read(&manifest_path).unwrap(), before);
    }

    #[test]
    fn creation_and_rotation_publish_only_durable_geometry() {
        use crate::wal::writer::FAIL_NEXT_SYNC;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        FAIL_NEXT_SYNC.with(|flag| flag.set(true));
        assert!(VersionSet::create_db_with_cache(path, 3, 5, None).is_err());
        assert!(!path.join("CURRENT").exists());
        let mut vs = VersionSet::create_db_with_cache(path, 3, 5, None).unwrap();
        let first = vs.manifest_number();
        vs.edits_since_snapshot = 1000;
        vs.maybe_compact_manifest();
        assert_ne!(vs.manifest_number(), first);
        drop(vs);
        let recovered = VersionSet::recover_with_cache(path, 3, 5, None).unwrap();
        assert_eq!(recovered.geometry(), (3, 5));
        let manifest = path.join(format!("MANIFEST-{:06}", recovered.manifest_number()));
        drop(recovered);
        let mut reader = WalReader::new(&manifest).unwrap();
        assert_eq!(
            reader.read_record().unwrap().unwrap(),
            DBHeader::new(3, 5).unwrap().encode()
        );
    }

    #[test]
    fn invalid_header_stops_recovery_before_tail_repair() {
        for (version, levels) in [(4u32, 7u32), (6, 7), (5, 1), (5, 17), (5, u32::MAX)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path();
            let manifest = path.join("MANIFEST-000001");
            let mut bytes = DBHeader::new(2, 0).unwrap().encode();
            bytes[4..8].copy_from_slice(&version.to_le_bytes());
            bytes[8..12].copy_from_slice(&levels.to_le_bytes());
            let mut writer = WalWriter::new(&manifest).unwrap();
            writer.add_record(&bytes).unwrap();
            writer.sync().unwrap();
            drop(writer);
            let mut original = fs::read(&manifest).unwrap();
            original.extend([1, 2]);
            fs::write(&manifest, &original).unwrap();
            fs::write(path.join("CURRENT"), b"MANIFEST-000001\n").unwrap();
            for result in [
                VersionSet::recover(path, 2),
                VersionSet::recover_read_only_with_cache(path, None),
            ] {
                assert_eq!(result.err().unwrap().kind(), crate::ErrorKind::Corruption);
            }
            assert_eq!(fs::read(manifest).unwrap(), original);
        }
    }

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
        assert_eq!(err.kind(), ErrorKind::Corruption);
        assert!(err.to_string().contains("999999.sst"));

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

    /// `log_and_apply` must reject the same malformed file-number streams
    /// that recovery rejects, or a persisted edit makes the next open fail.
    #[test]
    fn test_log_and_apply_matches_recovery_file_number_validation() {
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        let build_sst = |number: u64, key: &[u8]| {
            let mut builder = TableBuilder::new(
                &path.join(format!("{:06}.sst", number)),
                TableBuildOptions::default(),
            )
            .unwrap();
            builder
                .add(
                    InternalKey::new(key, 1, ValueType::Value).as_bytes(),
                    b"value",
                )
                .unwrap();
            let result = builder.finish().unwrap();
            FileMetaData {
                number,
                file_size: result.file_size,
                smallest_key: result.smallest_key.unwrap_or_default(),
                largest_key: result.largest_key.unwrap_or_default(),
                has_range_deletions: result.has_range_deletions,
            }
        };

        let mut vs = VersionSet::create(path, 7).unwrap();
        let meta1 = build_sst(1, b"a");
        let meta2 = build_sst(2, b"b");

        let mut add = VersionEdit::new();
        add.set_next_file_number(3);
        add.add_file(0, meta1.clone());
        vs.log_and_apply(add).unwrap();

        // A number that is already live cannot be added again.
        let mut duplicate_live = VersionEdit::new();
        duplicate_live.add_file(0, meta1.clone());
        let err = vs.log_and_apply(duplicate_live).unwrap_err();
        assert!(err.to_string().contains("already live"));
        assert_eq!(vs.current().total_files(), 1);
        assert!(!vs.is_poisoned());

        // The same number twice in one edit is rejected as well.
        let mut duplicate_in_edit = VersionEdit::new();
        duplicate_in_edit.add_file(0, meta2.clone());
        duplicate_in_edit.add_file(0, meta2.clone());
        let err = vs.log_and_apply(duplicate_in_edit).unwrap_err();
        assert!(err.to_string().contains("already live"));
        assert_eq!(vs.current().total_files(), 1);

        // Deletions are applied first: a trivial move that removes and
        // re-adds the same number remains valid.
        let mut trivial_move = VersionEdit::new();
        trivial_move.delete_file(0, 1);
        trivial_move.add_file(1, meta1);
        vs.log_and_apply(trivial_move).unwrap();
        assert_eq!(vs.current().level_files(0).len(), 0);
        assert_eq!(vs.current().level_files(1).len(), 1);
    }

    /// Already-poisoned writers must fail-closed on `sync_manifest` so
    /// post-apply cleanup cannot treat a later-sync success as permission
    /// to unlink inputs or WALs.
    #[test]
    fn test_sync_manifest_fails_closed_when_poisoned() {
        let dir = tempfile::tempdir().unwrap();
        let mut vs = VersionSet::create(dir.path(), 7).unwrap();

        let mut edit = VersionEdit::new();
        edit.set_last_sequence(1);
        edit.set_next_file_number(vs.next_file_number());
        vs.log_and_apply(edit).unwrap();

        // Simulate rotation/old-writer sync poison after a successful apply.
        vs.poisoned.store(true, Ordering::Release);
        let err = vs.sync_manifest().unwrap_err();
        assert!(
            err.to_string().contains("poisoned"),
            "sync_manifest must not succeed while poisoned, got {err}"
        );
        // Second attempt still fail-closed (fsyncgate must not soft-recover).
        assert!(vs.sync_manifest().is_err());
    }

    /// The file-number allocator must never move backwards, even if an edit
    /// carries a stale (lower) `next_file_number` — a regression would
    /// re-issue numbers of files that may still exist on disk.
    #[test]
    fn test_next_file_number_never_regresses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        let mut vs = VersionSet::create(path, 7).unwrap();
        let reserved = vs.file_numbers.allocate().unwrap();

        let mut stale_edit = VersionEdit::new();
        stale_edit.set_next_file_number(reserved); // stale: lower than current
        vs.log_and_apply(stale_edit).unwrap();
        assert_eq!(
            vs.next_file_number(),
            reserved + 1,
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

        let vs = VersionSet::create(path, 7).unwrap();

        // Allocate file numbers
        let n1 = vs.file_numbers.allocate().unwrap();
        let n2 = vs.file_numbers.allocate().unwrap();
        assert!(n1 < n2);

        let mut edit = VersionEdit::new();
        edit.set_last_sequence(42);
        edit.set_next_file_number(vs.next_file_number());
        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();
        assert_eq!(decoded.last_sequence, Some(42));
    }

    /// L1+ files must stay pairwise disjoint (binary-search read invariant):
    /// a live edit producing an overlap is rejected before persisting, and a
    /// persisted overlapping record fails the next recovery.
    #[test]
    fn test_overlapping_l1_files_rejected_live_and_on_recovery() {
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        fn build_sst(dir: &Path, number: u64, first: &[u8], last: &[u8]) -> FileMetaData {
            let path = dir.join(format!("{number:06}.sst"));
            let mut builder = TableBuilder::new(
                &path,
                TableBuildOptions {
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            for key in [first, last] {
                let ik = InternalKey::new(key, 1, ValueType::Value).into_bytes();
                builder.add(&ik, b"v").unwrap();
            }
            let result = builder.finish().unwrap();
            FileMetaData {
                number,
                file_size: result.file_size,
                smallest_key: result.smallest_key.unwrap(),
                largest_key: result.largest_key.unwrap(),
                has_range_deletions: false,
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let mut vs = VersionSet::create(path, 7).unwrap();

        let wide = build_sst(path, 10, b"a", b"z");
        let inner = build_sst(path, 11, b"b", b"c");

        let mut edit = VersionEdit::new();
        edit.add_file(1, wide.clone());
        vs.log_and_apply(edit).unwrap();

        // Live apply: an edit creating an L1 overlap is rejected wholesale.
        let mut overlap_edit = VersionEdit::new();
        overlap_edit.add_file(1, inner.clone());
        let err = vs.log_and_apply(overlap_edit).unwrap_err();
        assert!(
            err.to_string().contains("overlapping key ranges"),
            "unexpected error: {err}"
        );
        assert_eq!(vs.current().level_files(1).len(), 1, "edit must not apply");
        assert!(!vs.is_poisoned(), "clean rejection must not poison");

        // A persisted overlapping record (bypassing the live check, as a
        // malformed-but-checksum-valid MANIFEST would) fails the next open.
        {
            let mut bad_edit = VersionEdit::new();
            bad_edit.add_file(1, inner);
            let encoded = bad_edit.encode();
            let mut w = vs.manifest_writer.lock();
            if let Some(ref mut writer) = *w {
                writer.add_record(&encoded).unwrap();
                writer.sync().unwrap();
            }
        }
        drop(vs);
        let err = match VersionSet::recover(path, 7) {
            Ok(_) => panic!("recovery must reject overlapping L1 files"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("overlapping key ranges"),
            "unexpected recovery error: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn referenced_sst_open_preserves_io_errors() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let db = crate::DB::open(crate::DbOptions::default(), dir.path()).unwrap();
        db.put(b"a", b"value").unwrap();
        db.close().unwrap();
        let sst = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().is_some_and(|ext| ext == "sst"))
            .unwrap();
        let original = sst.with_extension("saved");
        fs::rename(&sst, &original).unwrap();
        symlink(sst.file_name().unwrap(), &sst).unwrap();

        let error = crate::DB::open_read_only(dir.path()).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Io);
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(libc::ELOOP)
        );
        fs::remove_file(&sst).unwrap();
        fs::rename(original, sst).unwrap();
        let reader = crate::DB::open_read_only(dir.path()).unwrap();
        assert_eq!(reader.get(b"a").unwrap().as_deref(), Some(&b"value"[..]));
    }

    #[test]
    fn mismatched_sst_size_rejected_on_apply_and_recovery() {
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let mut vs = VersionSet::create(path, 7).unwrap();
        let mut builder =
            TableBuilder::new(&path.join("000010.sst"), TableBuildOptions::default()).unwrap();
        let key = InternalKey::new(b"k", 1, ValueType::Value).into_bytes();
        builder.add(&key, b"v").unwrap();
        let result = builder.finish().unwrap();
        let mut edit = VersionEdit::new();
        edit.add_file(
            0,
            FileMetaData {
                number: 10,
                file_size: result.file_size + 1,
                smallest_key: key.clone(),
                largest_key: key,
                has_range_deletions: false,
            },
        );

        assert_eq!(
            vs.log_and_apply(edit.clone()).unwrap_err().kind(),
            ErrorKind::Corruption
        );
        assert_eq!(vs.current().total_files(), 0);
        vs.manifest_writer
            .lock()
            .as_mut()
            .unwrap()
            .add_record(&edit.encode())
            .unwrap();
        vs.sync_manifest().unwrap();
        drop(vs);
        let error = crate::DB::open_read_only(path).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Corruption);
        assert!(error.message().contains("file contains"));
    }

    #[test]
    fn malformed_or_inverted_file_bounds_are_rejected_on_apply_and_recovery() {
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let mut vs = VersionSet::create(path, 7).unwrap();

        let sst_path = path.join("000010.sst");
        let mut builder = TableBuilder::new(&sst_path, TableBuildOptions::default()).unwrap();
        let ik_a = InternalKey::new(b"a", 1, ValueType::Value).into_bytes();
        let ik_z = InternalKey::new(b"z", 1, ValueType::Value).into_bytes();
        builder.add(&ik_a, b"va").unwrap();
        builder.add(&ik_z, b"vz").unwrap();
        let res = builder.finish().unwrap();

        let cases = [
            // Empty smallest_key
            FileMetaData {
                number: 10,
                file_size: res.file_size,
                smallest_key: Vec::new(),
                largest_key: ik_z.clone(),
                has_range_deletions: false,
            },
            // Truncated (< 8B) largest_key
            FileMetaData {
                number: 10,
                file_size: res.file_size,
                smallest_key: ik_a.clone(),
                largest_key: b"short".to_vec(),
                has_range_deletions: false,
            },
            // Invalid ValueType byte in trailer
            FileMetaData {
                number: 10,
                file_size: res.file_size,
                smallest_key: {
                    let mut bad = ik_a.clone();
                    *bad.last_mut().unwrap() = 0x00;
                    bad
                },
                largest_key: ik_z.clone(),
                has_range_deletions: false,
            },
            // Inverted smallest_key > largest_key
            FileMetaData {
                number: 10,
                file_size: res.file_size,
                smallest_key: ik_z,
                largest_key: ik_a,
                has_range_deletions: false,
            },
        ];

        for bad_meta in &cases {
            let mut edit = VersionEdit::new();
            edit.add_file(1, bad_meta.clone());
            let err = vs.log_and_apply(edit).unwrap_err();
            assert_eq!(
                err.kind(),
                ErrorKind::Corruption,
                "expected Corruption, got {err:?}"
            );
            assert!(!vs.is_poisoned());
        }

        // Persisted malformed bounds in MANIFEST also fail recovery with Corruption.
        {
            let mut bad_edit = VersionEdit::new();
            bad_edit.add_file(1, cases[3].clone());
            let encoded = bad_edit.encode();
            let mut w = vs.manifest_writer.lock();
            let writer = w.as_mut().unwrap();
            writer.add_record(&encoded).unwrap();
            writer.sync().unwrap();
        }
        drop(vs);
        let err = match VersionSet::recover(path, 7) {
            Ok(_) => panic!("recovery must reject inverted FileMetaData bounds"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), ErrorKind::Corruption);
    }

    #[test]
    fn dir_sync_failure_blocks_following_sst_directory_sync() {
        use std::{sync::mpsc, thread, time::Duration};

        let dir = tempfile::tempdir().unwrap();
        let vs = VersionSet::create(dir.path(), 7).unwrap();
        let handle = vs.dir_sync_handle();
        let poison = vs.poison_flag();
        let (start_tx, start_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        thread::scope(|scope| {
            let next_handle = handle.clone();
            let next_poison = poison.clone();
            scope.spawn(move || {
                start_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                ready_tx.send(()).unwrap();
                result_tx
                    .send(sync_new_sst_parent_dir(&next_handle, &next_poison))
                    .unwrap();
            });
            let checked_handle = handle.clone();
            BEFORE_SYNC_POISON.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    start_tx.send(()).unwrap();
                    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    assert!(
                        checked_handle.try_lock().is_none(),
                        "failed directory sync must retain the handle until poison is published"
                    );
                }));
            });
            FAIL_NEXT_SST_DIR_SYNC.with(|fail| fail.set(true));
            assert_eq!(
                sync_new_sst_parent_dir(&handle, &poison)
                    .unwrap_err()
                    .kind(),
                ErrorKind::Io
            );
            assert_eq!(
                result_rx
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap()
                    .unwrap_err()
                    .kind(),
                ErrorKind::Background
            );
        });
    }

    #[test]
    fn recovery_rejects_manifest_without_version_edits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let mut vs = VersionSet::create(path, 7).unwrap();
        vs.edits_since_snapshot = 1000;
        vs.maybe_compact_manifest();
        let manifest = path.join(format!("MANIFEST-{:06}", vs.manifest_number()));
        drop(vs);

        let full = fs::read(&manifest).unwrap();
        // Record #0 is 7-byte WAL header + 16-byte DBHeader = 23 bytes.
        for truncated_len in [23usize, full.len() - 4] {
            fs::write(&manifest, &full[..truncated_len]).unwrap();
            let before = fs::read(&manifest).unwrap();
            for result in [
                VersionSet::recover_read_only_with_cache(path, None),
                VersionSet::recover(path, 7),
            ] {
                assert_eq!(result.err().unwrap().kind(), ErrorKind::Corruption);
                assert_eq!(fs::read(&manifest).unwrap(), before);
            }
        }
    }

    #[test]
    fn file_numbers_advance_past_manifest_log_and_sst_numbers() {
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let manifest_path = path.join("MANIFEST-000010");
        let sst_meta = {
            let mut b =
                TableBuilder::new(&path.join("000012.sst"), TableBuildOptions::default()).unwrap();
            let ik = InternalKey::new(b"a", 1, ValueType::Value);
            b.add(ik.as_bytes(), b"va").unwrap();
            let res = b.finish().unwrap();
            FileMetaData {
                number: 12,
                file_size: res.file_size,
                smallest_key: res.smallest_key.unwrap(),
                largest_key: res.largest_key.unwrap(),
                has_range_deletions: false,
            }
        };

        let mut writer = WalWriter::new(&manifest_path).unwrap();
        writer
            .add_record(&DBHeader::new(7, 0).unwrap().encode())
            .unwrap();
        let mut edit = VersionEdit::new();
        edit.set_log_number(8);
        edit.set_last_sequence(1);
        // Deliberately omit `next_file_number` (or leave it stale below 10/12).
        edit.set_next_file_number(4);
        edit.add_file(1, sst_meta);
        writer.add_record(&edit.encode()).unwrap();
        writer.sync().unwrap();
        drop(writer);
        fs::write(path.join("CURRENT"), b"MANIFEST-000010\n").unwrap();

        let mut vs = VersionSet::recover(path, 7).unwrap();
        assert!(vs.next_file_number() >= 13);
        vs.edits_since_snapshot = 1000;
        let mut meta_edit = VersionEdit::new();
        meta_edit.set_log_number(20);
        vs.apply_metadata(meta_edit).unwrap();
        assert!(vs.next_file_number() >= 21);
        let active_manifest = path.join(format!("MANIFEST-{:06}", vs.manifest_number()));
        assert!(active_manifest.exists());
        drop(vs);

        let reopened = VersionSet::recover(path, 7).unwrap();
        assert_eq!(reopened.current().total_files(), 1);
    }

    #[test]
    fn incremental_install_validates_neighbors_and_preserves_live_set_on_rejection() {
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let build_range = |number: u64, lo: &[u8], hi: &[u8]| {
            let mut b = TableBuilder::new(
                &path.join(format!("{number:06}.sst")),
                TableBuildOptions {
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            b.add(
                InternalKey::new(lo, 10, ValueType::Value).as_bytes(),
                b"vlo",
            )
            .unwrap();
            if hi != lo {
                b.add(
                    InternalKey::new(hi, 10, ValueType::Value).as_bytes(),
                    b"vhi",
                )
                .unwrap();
            }
            let res = b.finish().unwrap();
            FileMetaData {
                number,
                file_size: res.file_size,
                smallest_key: res.smallest_key.unwrap(),
                largest_key: res.largest_key.unwrap(),
                has_range_deletions: false,
            }
        };

        let mut vs = VersionSet::create(path, 7).unwrap();
        let mut seed = VersionEdit::new();
        for i in 0..32u64 {
            let k1 = format!("l2_{:04}_a", i * 2);
            let k2 = format!("l2_{:04}_z", i * 2);
            seed.add_file(2, build_range(100 + i, k1.as_bytes(), k2.as_bytes()));
        }
        seed.add_file(1, build_range(10, b"b0", b"d0"));
        seed.add_file(1, build_range(11, b"f0", b"h0"));
        seed.add_file(1, build_range(12, b"j0", b"l0"));
        vs.log_and_apply(seed).unwrap();

        // 1. Left-neighbor overlap (reaches `d0`) and right-neighbor overlap
        //    (reaches `f0`) must both be rejected without polluting `live_file_numbers`.
        let bad_left = build_range(20, b"d0", b"e0");
        let mut e1 = VersionEdit::new();
        e1.add_file(1, bad_left);
        assert_eq!(
            vs.log_and_apply(e1).unwrap_err().kind(),
            ErrorKind::Corruption
        );

        // File number 20 must NOT be stuck in `live_file_numbers` after rejection:
        // rewriting 20 as a valid disjoint gap file `("d1".."e9")` must succeed!
        let bad_right = build_range(21, b"e1", b"f0");
        let mut e2 = VersionEdit::new();
        e2.add_file(1, bad_right);
        assert_eq!(
            vs.log_and_apply(e2).unwrap_err().kind(),
            ErrorKind::Corruption
        );

        // Intra-edit overlap between two newly added L1 files is also rejected cleanly.
        let intra_a = build_range(22, b"d1", b"e5");
        let intra_b = build_range(23, b"e5", b"e9");
        let mut e3 = VersionEdit::new();
        e3.add_file(1, intra_a);
        e3.add_file(1, intra_b);
        assert_eq!(
            vs.log_and_apply(e3).unwrap_err().kind(),
            ErrorKind::Corruption
        );

        // Valid insertion reusing number 22 into the `("d0", "f0")` gap, plus
        // deleting 11 and inserting 23 into the freed `("e9", "j0")` span:
        let mut ok = VersionEdit::new();
        ok.delete_file(1, 11);
        ok.add_file(1, build_range(22, b"d1", b"e5"));
        ok.add_file(1, build_range(23, b"e6", b"i9"));
        vs.log_and_apply(ok).unwrap();
        vs.sync_manifest().unwrap();
        drop(vs);

        let recovered = VersionSet::recover(path, 7).unwrap();
        let l1_nums: Vec<u64> = recovered
            .current()
            .level_files(1)
            .iter()
            .map(|f| f.meta.number)
            .collect();
        assert_eq!(l1_nums, vec![10, 22, 23, 12]);
        assert_eq!(recovered.current().level_files(2).len(), 32);
    }
}
