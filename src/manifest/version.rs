//! Version: an immutable snapshot of which SST files exist at each level.

use std::{cmp::Reverse, fmt, ops::Deref, sync::Arc};

use crate::manifest::version_edit::FileMetaData;
use crate::sst::table_reader::TableReader;
use crate::types::compare_internal_key;

/// An open SST file with its metadata.
#[derive(Clone)]
pub struct TableFile {
    pub meta: FileMetaData,
    pub reader: Arc<TableReader>,
}

/// A level's ordered files and the subset of readers that contain range deletions.
/// A tombstone can cover keys beyond its file's point bounds.
#[derive(Clone, Default)]
pub(crate) struct LevelFiles {
    files: Vec<TableFile>,
    range_readers: Vec<Arc<TableReader>>,
}

impl Deref for LevelFiles {
    type Target = [TableFile];

    fn deref(&self) -> &[TableFile] {
        &self.files
    }
}

impl LevelFiles {
    pub(super) fn insert(&mut self, index: usize, file: TableFile) {
        if file.meta.has_range_deletions {
            self.range_readers.push(file.reader.clone());
        }
        self.files.insert(index, file);
    }

    pub(super) fn remove_file(&mut self, number: u64) -> bool {
        let Some(index) = self
            .files
            .iter()
            .position(|file| file.meta.number == number)
        else {
            return false;
        };
        let removed = self.files.remove(index);
        if removed.meta.has_range_deletions {
            self.range_readers
                .retain(|reader| !Arc::ptr_eq(reader, &removed.reader));
        }
        true
    }
}

/// An immutable snapshot of the database's SST file set.
///
/// Each Version contains, for every level, the list of SST files.
/// Versions are reference-counted and live as long as any reader
/// or iterator holds a reference.
#[derive(Clone)]
pub struct Version {
    /// Files at each level. Level 0 is sorted newest-first.
    /// Levels 1+ are sorted by key range (no overlap within a level).
    /// Unchanged levels are shared between versions. Mutation uses
    /// `Arc::make_mut`, preserving every version already pinned by readers.
    pub files: Vec<Arc<LevelFiles>>,
    /// Number of levels.
    pub num_levels: usize,
}

impl Version {
    /// Create an empty version with the given number of levels.
    pub fn new(num_levels: usize) -> Self {
        Self {
            files: (0..num_levels)
                .map(|_| Arc::new(LevelFiles::default()))
                .collect(),
            num_levels,
        }
    }

    /// Get files at a specific level.
    pub fn level_files(&self, level: usize) -> &[TableFile] {
        &self.files[level]
    }

    pub(super) fn sort_recovered_files(&mut self) {
        Arc::make_mut(&mut self.files[0])
            .files
            .sort_by_key(|file| Reverse(file.meta.number));
        for level in &mut self.files[1..] {
            Arc::make_mut(level)
                .files
                .sort_by(|a, b| compare_internal_key(&a.meta.smallest_key, &b.meta.smallest_key));
        }
    }

    pub(crate) fn range_readers(&self, level: usize) -> &[Arc<TableReader>] {
        &self.files[level].range_readers
    }

    pub fn total_sst_size(&self) -> u64 {
        self.files
            .iter()
            .flat_map(|level| level.iter())
            .map(|file| file.meta.file_size)
            .fold(0, u64::saturating_add)
    }

    /// Total number of files across all levels (test helper).
    #[cfg(test)]
    pub(crate) fn total_files(&self) -> usize {
        self.files.iter().map(|f| f.len()).sum()
    }

    /// Number of L0 files.
    pub fn l0_file_count(&self) -> usize {
        self.files[0].len()
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Version {{ levels: [")?;
        for (i, level) in self.files.iter().enumerate() {
            if !level.is_empty() {
                write!(f, "L{}: {} files, ", i, level.len())?;
            }
        }
        write!(f, "] }}")
    }
}
