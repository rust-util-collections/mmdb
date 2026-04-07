//! VersionSet: manages the MANIFEST file and the chain of Versions.

use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cache::table_cache::TableCache;
use crate::error::{Error, Result};
use crate::manifest::version::{TableFile, Version};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::sst::table_reader::TableReader;
use crate::types::{SequenceNumber, compare_internal_key};
use crate::wal::{WalReader, WalWriter};
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
    /// MANIFEST writer.
    manifest_writer: Option<WalWriter>,
    /// Table cache for opening SST readers.
    table_cache: Option<Arc<TableCache>>,
    /// Count of edits since last MANIFEST snapshot (for Phase I compaction).
    edits_since_snapshot: u64,
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
            manifest_writer: Some(manifest_writer),
            table_cache,
            edits_since_snapshot: 0,
        };

        // Write initial snapshot edit
        let mut edit = VersionEdit::new();
        edit.set_next_file_number(vs.next_file_number);
        edit.set_last_sequence(vs.last_sequence);
        vs.log_and_apply(edit).c(d!())?;

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
        let manifest_name = std::fs::read_to_string(&current_path)
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

        for record in reader.iter() {
            let data = match record {
                Ok(d) => d,
                Err(e) => {
                    // Tolerate a truncated tail record: this is expected when the
                    // previous process exited without syncing the MANIFEST writer
                    // (e.g. Box::leak singleton pattern, kill -9, power loss).
                    // Matches the WAL recovery strategy in db.rs.
                    tracing::warn!("MANIFEST: skipping truncated tail record: {}", e);
                    break;
                }
            };
            let edit = VersionEdit::decode(&data).c(d!())?;

            if let Some(n) = edit.next_file_number {
                next_file_number = n;
            }
            if let Some(n) = edit.log_number {
                log_number = n;
            }
            if let Some(s) = edit.last_sequence {
                last_sequence = s;
            }

            // Track additions
            for (level, meta) in &edit.new_files {
                let level = *level as usize;
                if level < num_levels {
                    live_files.insert(meta.number, (level, meta.clone()));
                }
            }

            // Track deletions
            for (_level, file_number) in &edit.deleted_files {
                live_files.remove(file_number);
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
        version.files[0].sort_by(|a, b| b.meta.number.cmp(&a.meta.number));
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
            manifest_writer: Some(manifest_writer),
            table_cache,
            edits_since_snapshot: 0,
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
    pub fn log_and_apply(&mut self, edit: VersionEdit) -> Result<()> {
        // Build new version from current + edit FIRST, before persisting.
        // This ensures that if an SST fails to open, the MANIFEST is not
        // polluted with an edit referencing a broken file.
        let mut new_version = (*self.current).clone();

        for (level, meta) in &edit.new_files {
            let level = *level as usize;
            if level < self.num_levels {
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
        }

        for (level, file_number) in &edit.deleted_files {
            let level = *level as usize;
            if level < self.num_levels {
                new_version.files[level].retain(|f| f.meta.number != *file_number);
            }
        }

        // Version built successfully — now persist to MANIFEST
        let encoded = edit.encode();
        if let Some(ref mut writer) = self.manifest_writer {
            writer.add_record(&encoded).c(d!())?;
            writer.sync().c(d!())?;
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

        // Check if MANIFEST needs compaction
        self.maybe_compact_manifest().c(d!())?;

        Ok(())
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

    pub fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence
    }

    pub fn set_last_sequence(&mut self, seq: SequenceNumber) {
        self.last_sequence = seq;
    }

    /// Rewrite the MANIFEST with a full snapshot of the current version.
    /// This prevents unbounded MANIFEST growth.
    pub fn maybe_compact_manifest(&mut self) -> Result<()> {
        const MANIFEST_COMPACTION_THRESHOLD: u64 = 1000;

        if self.edits_since_snapshot < MANIFEST_COMPACTION_THRESHOLD {
            return Ok(());
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
        let mut new_writer = WalWriter::new(&new_manifest_path).c(d!())?;

        // Write the snapshot edit
        let encoded = snapshot_edit.encode();
        new_writer.add_record(&encoded).c(d!())?;
        new_writer.sync().c(d!())?;

        // Update CURRENT to point to new manifest
        Self::set_current_file(&self.db_path, new_manifest_number).c(d!())?;

        // Delete old manifest
        let old_manifest_path = self
            .db_path
            .join(format!("MANIFEST-{:06}", self.manifest_number));
        if let Err(e) = fs::remove_file(&old_manifest_path) {
            tracing::warn!(
                "failed to remove old manifest {}: {}",
                old_manifest_path.display(),
                e
            );
        }

        self.manifest_number = new_manifest_number;
        self.manifest_writer = Some(new_writer);
        self.edits_since_snapshot = 0;

        Ok(())
    }

    fn set_current_file(db_path: &Path, manifest_number: u64) -> Result<()> {
        let contents = format!("MANIFEST-{:06}\n", manifest_number);
        let tmp_path = db_path.join("CURRENT.tmp");

        // Write and fsync the temp file before rename to ensure durability
        {
            let file = fs::File::create(&tmp_path).c(d!())?;
            let mut writer = BufWriter::new(file);
            writer.write_all(contents.as_bytes()).c(d!())?;
            writer.flush().c(d!())?;
            writer.get_ref().sync_all().c(d!())?;
        }

        // Atomic rename
        fs::rename(&tmp_path, db_path.join("CURRENT")).c(d!())?;

        // Fsync the directory to persist the rename metadata
        Self::fsync_directory(db_path).c(d!())?;

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
        let current_content = std::fs::read_to_string(path.join("CURRENT")).unwrap();
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
            let mut vs = VersionSet::create(path, 7).unwrap();
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
            if let Some(ref mut writer) = vs.manifest_writer {
                writer.add_record(&encoded).unwrap();
                writer.sync().unwrap();
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
