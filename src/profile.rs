//! Optional thread-local measurements for the engine workload profiles.

/// Time the surrounding scope on the thread performing the operation.
/// With the feature off the macro expands to an empty scope declaration.
macro_rules! time_scope {
    ($field:ident $(, $event:ident)?) => {
        #[cfg(feature = "bench-metrics")]
        let _write_profile_timer = $crate::profile::Timer::new(|p, ns| {
            p.$field = p.$field.saturating_add(ns);
            $(p.$event += 1;)?
        });
    };
}
pub(crate) use time_scope;

macro_rules! timed {
    ($field:ident, $event:ident, $expr:expr) => {{
        $crate::profile::time_scope!($field, $event);
        $expr
    }};
}
pub(crate) use timed;

#[cfg(feature = "bench-metrics")]
mod enabled {
    use std::{cell::RefCell, time::Instant};

    /// Accumulated write-path measurements for one OS thread, across DB handles.
    ///
    /// Group counters belong to the leader thread. A follower's queue wait can
    /// overlap a leader's WAL or memtable work, so stage totals across threads
    /// cannot be added to obtain elapsed process time.
    #[derive(Clone, Debug, Default)]
    #[non_exhaustive]
    pub struct ThreadWriteProfile {
        /// Groups drained by this thread as write leader.
        pub groups: u64,
        /// Requests in those groups, including fences and later admission refusals.
        pub requests: u64,
        /// Encoded batch payload bytes drained, excluding WAL headers.
        pub payload_bytes: u64,
        /// Largest drained group, in requests.
        pub max_group_requests: u64,
        /// Largest drained group, in encoded payload bytes.
        pub max_group_bytes: u64,
        /// This thread's requests that waited at least once on the write queue.
        pub queue_waits: u64,
        /// Returns from write-queue condition waits, including repeated wakes.
        pub queue_wakeups: u64,
        /// Time in those condition waits, including mutex reacquisition.
        pub queue_wait_ns: u64,
        /// Time appending WAL records for admitted requests.
        pub wal_append_ns: u64,
        /// WAL record append attempts.
        pub wal_appends: u64,
        /// Time flushing a group's async WAL bytes to the OS, excluding writeback hints.
        pub wal_flush_ns: u64,
        /// Async WAL flush attempts by the group leader.
        pub wal_flushes: u64,
        /// Time in group or fence WAL data syncs. Sealing a WAL is timed in freeze_ns.
        pub wal_sync_ns: u64,
        /// WAL data-sync attempts by group leaders or fences.
        pub wal_syncs: u64,
        /// Time applying admitted entries to the active memtable and publishing visibility.
        pub apply_ns: u64,
        /// Time freezing a nonempty generation, including sealing the old WAL.
        pub freeze_ns: u64,
        /// Generation freeze attempts.
        pub freezes: u64,
        /// Completed condition waits for immutable capacity, including repeated wakes.
        pub immutable_waits: u64,
        /// Time spent waiting on the flush queue for immutable capacity.
        pub immutable_wait_ns: u64,
        /// Sleeps requested because the L0 count reached the slowdown threshold.
        pub l0_slowdowns: u64,
        /// Wall time spent in the L0 slowdown sleep.
        pub l0_slowdown_ns: u64,
        /// Times the observed L0 count triggered the stop path.
        pub l0_stops: u64,
        /// Stop-path rechecks that still required a foreground L0 drain.
        pub l0_drains: u64,
        /// Wall time in the L0 stop path, including writer-barrier wait and inline drain.
        pub l0_stop_ns: u64,
    }

    thread_local! {
        static PROFILE: RefCell<ThreadWriteProfile> = RefCell::new(ThreadWriteProfile::default());
    }

    /// Read and reset the current thread's counters. Sample on each worker
    /// before and after the measured interval. Sum counts/times across workers
    /// and combine maxima with `max`.
    pub fn take_thread_write_profile() -> ThreadWriteProfile {
        PROFILE.with(RefCell::take)
    }

    pub(crate) fn record(update: impl FnOnce(&mut ThreadWriteProfile)) {
        PROFILE.with_borrow_mut(update);
    }

    pub(crate) struct Timer {
        start: Instant,
        record: fn(&mut ThreadWriteProfile, u64),
    }

    impl Timer {
        pub(crate) fn new(record: fn(&mut ThreadWriteProfile, u64)) -> Self {
            Self {
                start: Instant::now(),
                record,
            }
        }
    }

    impl Drop for Timer {
        fn drop(&mut self) {
            let ns = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
            record(|p| (self.record)(p, ns));
        }
    }
}

#[cfg(feature = "bench-metrics")]
pub use enabled::{ThreadWriteProfile, take_thread_write_profile};
#[cfg(feature = "bench-metrics")]
pub(crate) use enabled::{Timer, record};

#[cfg(all(test, feature = "bench-metrics"))]
mod tests {
    use super::take_thread_write_profile;
    use crate::{DB, DbOptions, DurabilityMode, ErrorKind, WriteBatch, WriteOptions};

    #[test]
    fn classifies_wal_stages_and_flushes_on_the_measured_thread() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        let mut first = WriteBatch::new();
        first.put(b"k", b"v");
        first.delete(b"missing");
        let mut second = WriteBatch::new();
        second.put(b"next", b"value");
        let first_bytes = first.payload.len() as u64;
        let second_bytes = second.payload.len() as u64;
        take_thread_write_profile();
        db.write(first).unwrap();
        db.sync_wal().unwrap();
        db.sync_wal().unwrap(); // Already covered, so the second fence skips the queue.
        let synced = WriteOptions::default().with_durability(DurabilityMode::WalSync);
        db.write_with_options(&synced, second).unwrap();
        db.flush().unwrap();

        let p = take_thread_write_profile();
        assert_eq!(p.groups, 3);
        assert_eq!(p.requests, 3);
        assert_eq!(p.max_group_requests, 1);
        assert_eq!(p.payload_bytes, first_bytes + second_bytes);
        assert_eq!(p.max_group_bytes, first_bytes.max(second_bytes));
        assert_eq!(p.wal_appends, 2);
        assert_eq!(p.wal_flushes, 1);
        assert_eq!(p.wal_syncs, 2);
        assert_eq!(p.freezes, 1);
        assert!(p.apply_ns > 0);
        assert!(p.wal_append_ns > 0);
        assert!(p.wal_flush_ns > 0);
        assert!(p.wal_sync_ns > 0);
        assert!(p.freeze_ns > 0);
        assert_eq!(p.queue_waits, 0);
        assert_eq!(p.immutable_waits, 0);
        assert_eq!(take_thread_write_profile().requests, 0);

        let worker = std::thread::scope(|s| {
            s.spawn(|| {
                db.delete(b"next").unwrap();
                take_thread_write_profile()
            })
            .join()
            .unwrap()
        });
        assert_eq!(worker.requests, 1);
        assert_eq!(take_thread_write_profile().requests, 0);
    }

    #[test]
    fn classifies_l0_slowdown_stop_and_immediate_busy() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions::default()
            .with_l0_compaction_trigger(usize::MAX)
            .with_l0_slowdown_trigger(1)
            .with_l0_stop_trigger(2);
        let db = DB::open(opts, dir.path()).unwrap();
        db.put(b"key", b"first").unwrap();
        db.flush().unwrap();
        take_thread_write_profile();
        let no_wait = WriteOptions::default().with_no_slowdown(true);
        let err = db
            .put_with_options(&no_wait, b"key", b"refused")
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Busy);
        let refused = take_thread_write_profile();
        assert_eq!(refused.groups, 0);
        assert_eq!(refused.l0_slowdowns, 0);
        assert_eq!(refused.l0_stops, 0);
        assert_eq!(refused.immutable_waits, 0);

        db.put(b"key", b"second").unwrap();
        db.flush().unwrap();
        let slowed = take_thread_write_profile();
        assert_eq!(slowed.l0_slowdowns, 1);
        assert!(slowed.l0_slowdown_ns > 0);
        assert_eq!(slowed.l0_stops, 0);

        db.put(b"key", b"third").unwrap();
        let stopped = take_thread_write_profile();
        assert_eq!(stopped.l0_slowdowns, 0);
        assert_eq!(stopped.l0_stops, 1);
        assert_eq!(stopped.l0_drains, 1);
        assert!(stopped.l0_stop_ns > 0);
    }
}
