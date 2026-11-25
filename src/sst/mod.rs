//! Sorted String Table (SST) implementation.
//!
//! An SST file is immutable once written. Structure:
//! - Data blocks (prefix-compressed key-value pairs)
//! - Blocked key and prefix Bloom filters
//! - Range-deletion leaves and exclusive-end directory
//! - Metaindex block
//! - Index leaves and separator directory
//! - Footer (fixed size, points to index and meta index)

use std::{
    cell::Cell,
    thread::{self, ThreadId},
};

use parking_lot::Mutex;

pub mod block;
pub mod block_builder;
pub mod filter;
pub mod format;
pub(crate) mod range;
pub mod table_builder;
pub mod table_reader;

/// Hard ceiling for single-block metadata (index, range-del, and bloom
/// filter blocks). The reader rejects any block above
/// `MAX_DECOMPRESSED_BLOCK_SIZE`; the margin leaves room for the restart
/// array and block framing.
pub(crate) const META_BLOCK_HARD_LIMIT: usize = table_reader::MAX_DECOMPRESSED_BLOCK_SIZE - 4096;

thread_local! {
    // Ordinary writes avoid consulting the per-DB builder registry.
    static BUILD_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Threads running SST build callbacks for this database. A close on one
/// of these threads would wait for its own flush or compaction.
#[derive(Default)]
pub(crate) struct SstBuilds(Mutex<Vec<ThreadId>>);

impl SstBuilds {
    pub fn enter(&self) -> SstBuildGuard<'_> {
        let thread = thread::current().id();
        self.0.lock().push(thread);
        BUILD_DEPTH.with(|depth| depth.set(depth.get() + 1));
        SstBuildGuard {
            builds: self,
            thread,
            local: std::marker::PhantomData,
        }
    }

    pub fn contains_current_thread(&self) -> bool {
        BUILD_DEPTH.get() != 0 && self.0.lock().contains(&thread::current().id())
    }
}

pub(crate) struct SstBuildGuard<'a> {
    builds: &'a SstBuilds,
    thread: ThreadId,
    // Drops decrement the registering thread's TLS depth.
    local: std::marker::PhantomData<*const ()>,
}

impl Drop for SstBuildGuard<'_> {
    fn drop(&mut self) {
        let mut threads = self.builds.0.lock();
        let index = threads
            .iter()
            .position(|id| *id == self.thread)
            .expect("registered SST build");
        threads.swap_remove(index);
        BUILD_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}
