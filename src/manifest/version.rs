//! Version: an immutable snapshot of which SST files exist at each level.

use std::sync::Arc;

use crate::manifest::version_edit::FileMetaData;
use crate::sst::table_reader::TableReader;

/// An open SST file with its metadata.
#[derive(Clone)]
pub struct TableFile {
    pub meta: FileMetaData,
    pub reader: Arc<TableReader>,
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
    pub files: Vec<Vec<TableFile>>,
    /// Number of levels.
    pub num_levels: usize,
}

impl Version {
    /// Create an empty version with the given number of levels.
    pub fn new(num_levels: usize) -> Self {
        Self {
            files: vec![Vec::new(); num_levels],
            num_levels,
        }
    }

    /// Get files at a specific level.
    pub fn level_files(&self, level: usize) -> &[TableFile] {
        &self.files[level]
    }

    /// Total number of files across all levels.
    pub fn total_files(&self) -> usize {
        self.files.iter().map(|f| f.len()).sum()
    }

    /// Number of L0 files.
    pub fn l0_file_count(&self) -> usize {
        self.files[0].len()
    }
}

impl std::fmt::Debug for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Version {{ levels: [")?;
        for (i, level) in self.files.iter().enumerate() {
            if !level.is_empty() {
                write!(f, "L{}: {} files, ", i, level.len())?;
            }
        }
        write!(f, "] }}")
    }
}
