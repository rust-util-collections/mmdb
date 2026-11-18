//! Sorted String Table (SST) implementation.
//!
//! An SST file is immutable once written. Structure:
//! - Data blocks (prefix-compressed key-value pairs)
//! - Meta block (bloom filter)
//! - Meta index block
//! - Index block (one entry per data block)
//! - Footer (fixed size, points to index and meta index)

use std::thread::{self, ThreadId};

use parking_lot::Mutex;

pub mod block;
pub mod block_builder;
pub mod filter;
pub mod format;
pub mod table_builder;
pub mod table_reader;

/// Hard ceiling for single-block metadata (index, range-del, and bloom
/// filter blocks). The reader rejects any block above
/// `MAX_DECOMPRESSED_BLOCK_SIZE`; the margin leaves room for the restart
/// array and block framing.
pub(crate) const META_BLOCK_HARD_LIMIT: usize = table_reader::MAX_DECOMPRESSED_BLOCK_SIZE - 4096;

/// Threads running SST build callbacks for this database. A close on one
/// of these threads would wait for its own flush or compaction.
#[derive(Default)]
pub(crate) struct SstBuilds(Mutex<Vec<ThreadId>>);

impl SstBuilds {
    pub fn enter(&self) -> SstBuildGuard<'_> {
        let thread = thread::current().id();
        self.0.lock().push(thread);
        SstBuildGuard {
            builds: self,
            thread,
        }
    }

    pub fn contains_current_thread(&self) -> bool {
        self.0.lock().contains(&thread::current().id())
    }
}

pub(crate) struct SstBuildGuard<'a> {
    builds: &'a SstBuilds,
    thread: ThreadId,
}

impl Drop for SstBuildGuard<'_> {
    fn drop(&mut self) {
        let mut threads = self.builds.0.lock();
        let index = threads
            .iter()
            .position(|id| *id == self.thread)
            .expect("registered SST build");
        threads.swap_remove(index);
    }
}
