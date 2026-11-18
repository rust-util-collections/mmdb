//! A single allocation sequence for WAL, MANIFEST and SST files.

use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;

use crate::error::{Error, Result};

pub(crate) struct FileNumbers(AtomicU64);

impl FileNumbers {
    pub fn new(next: u64) -> Self {
        Self(AtomicU64::new(next))
    }

    pub fn allocate(&self) -> Result<u64> {
        self.0
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::background("database file numbers exhausted"))
    }

    pub fn next(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn advance_to(&self, next: u64) {
        self.0.fetch_max(next, Ordering::Relaxed);
    }
}

/// Own exactly one flush or compaction's new SSTs until the caller takes
/// responsibility for installing them. Other jobs can allocate between them.
pub(crate) struct PendingSsts<'a> {
    directory: &'a Path,
    numbers: &'a FileNumbers,
    allocated: Mutex<Vec<u64>>,
}

impl<'a> PendingSsts<'a> {
    pub fn new(directory: &'a Path, numbers: &'a FileNumbers) -> Self {
        Self {
            directory,
            numbers,
            allocated: Mutex::default(),
        }
    }

    pub fn allocate(&self) -> Result<u64> {
        let number = self.numbers.allocate()?;
        self.allocated.lock().push(number);
        Ok(number)
    }

    pub fn keep(mut self) {
        self.allocated.get_mut().clear();
    }
}

impl Drop for PendingSsts<'_> {
    fn drop(&mut self) {
        for number in self.allocated.get_mut().drain(..) {
            let _ = fs::remove_file(self.directory.join(format!("{number:06}.sst")));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abandoned_job_only_removes_its_own_files() {
        let dir = tempfile::tempdir().unwrap();
        let numbers = FileNumbers::new(u64::MAX - 3);
        let first = PendingSsts::new(dir.path(), &numbers);
        let second = PendingSsts::new(dir.path(), &numbers);
        let make_file = |job: &PendingSsts<'_>| {
            let path = dir
                .path()
                .join(format!("{:06}.sst", job.allocate().unwrap()));
            fs::write(&path, []).unwrap();
            path
        };
        let a = make_file(&first);
        let b = make_file(&second);
        let c = make_file(&first);
        assert!(numbers.allocate().is_err());
        second.keep();
        drop(first);
        assert!(!a.exists() && b.exists() && !c.exists());
    }
}
