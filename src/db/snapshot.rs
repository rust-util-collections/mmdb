//! Registered MVCC read handles.
use super::{DB, DeadKeySweepScheduler, SnapshotList, read::ReadState};
use crate::{DBIterator, Error, PinnedSlice, ReadOptions, Result, SequenceNumber, types};
use std::{
    fmt,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

pub(super) static NEXT_DB_ID: AtomicU64 = AtomicU64::new(1);

/// RAII snapshot guard. Automatically releases the snapshot when dropped.
///
/// Every read option derived from this guard borrows its live registration.
///
/// ```compile_fail
/// # fn example(db: &mmdb::DB) -> mmdb::Result<()> {
/// let opts = db.snapshot().read_options();
/// db.get_with_options(&opts, b"key")?;
/// # Ok(()) }
/// ```
#[must_use]
pub struct Snapshot<'a> {
    db: &'a DB,
    seq: SequenceNumber,
}

impl<'a> Snapshot<'a> {
    pub(super) fn register(db: &'a DB) -> Self {
        Self {
            db,
            seq: db.snapshot_seq(),
        }
    }

    /// The sequence number for this snapshot.
    pub fn sequence(&self) -> SequenceNumber {
        self.seq
    }

    /// Return `ReadOptions` pre-configured to read at this snapshot.
    pub fn read_options(&self) -> ReadOptions<'_> {
        ReadOptions {
            snapshot: Some(self.into()),
            ..ReadOptions::default()
        }
    }

    /// Copy the visible value at this snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get_with_options(&self.read_options(), key)
    }

    /// Pin the visible bytes at this snapshot. See [`PinnedSlice`] for retention costs.
    pub fn get_pinned(&self, key: &[u8]) -> Result<Option<PinnedSlice>> {
        self.db.get_pinned_with_options(&self.read_options(), key)
    }

    /// Check point and range deletion visibility at this snapshot.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool> {
        self.db.contains_key_with_options(&self.read_options(), key)
    }

    /// Iterate the visible rows in key order.
    pub fn iter(&self) -> Result<DBIterator> {
        self.db.iter_with_options(&self.read_options())
    }

    /// Iterate the half-open range from the inclusive lower to the exclusive upper bound.
    pub fn iter_with_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<DBIterator> {
        self.db.iter_with_range(&self.read_options(), lower, upper)
    }

    /// Iterate rows whose user keys begin with the supplied bytes.
    pub fn iter_with_prefix(&self, prefix: &[u8]) -> Result<DBIterator> {
        self.db.iter_with_prefix(prefix, &self.read_options())
    }
}

/// A borrowed, registered read view for use in [`ReadOptions`].
#[derive(Clone, Copy)]
pub enum SnapshotRef<'a> {
    /// A registration borrowed from the originating DB handle.
    Borrowed(&'a Snapshot<'a>),
    /// A registration held by a cloneable snapshot lease.
    Shared(&'a ArcSnapshot),
}

impl SnapshotRef<'_> {
    pub(super) fn sequence_for(self, reader: &ReadState) -> Result<SequenceNumber> {
        let (id, seq) = match self {
            Self::Borrowed(s) => (s.db.read.db_id, s.seq),
            Self::Shared(s) => (s.0.db_id, s.0.seq),
        };
        if id != reader.db_id {
            return Err(Error::invalid_argument(
                "snapshot belongs to another database",
            ));
        }
        Ok(seq)
    }
}

impl fmt::Debug for SnapshotRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seq = match self {
            Self::Borrowed(s) => s.seq,
            Self::Shared(s) => s.0.seq,
        };
        f.debug_tuple("SnapshotRef").field(&seq).finish()
    }
}

impl<'a> From<&'a Snapshot<'a>> for SnapshotRef<'a> {
    fn from(snapshot: &'a Snapshot<'a>) -> Self {
        Self::Borrowed(snapshot)
    }
}

impl<'a> From<&'a ArcSnapshot> for SnapshotRef<'a> {
    fn from(snapshot: &'a ArcSnapshot) -> Self {
        Self::Shared(snapshot)
    }
}

struct SnapshotLease {
    seq: SequenceNumber,
    db_id: u64,
    reader: Weak<ReadState>,
    snapshots: Arc<SnapshotList>,
    sweep: Arc<DeadKeySweepScheduler>,
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        self.snapshots.release(self.seq);
        if let Some(reader) = self.reader.upgrade()
            && !reader.closed.load(Ordering::Acquire)
            && self.sweep.retry_after_snapshot_release()
        {
            reader.signal_compaction();
        }
    }
}

/// A shareable MVCC registration. Its last clone releases the snapshot.
/// It holds neither the directory lock nor SST handles.
#[must_use]
#[derive(Clone)]
pub struct ArcSnapshot(Arc<SnapshotLease>);

impl ArcSnapshot {
    pub(super) fn register(db: &DB) -> Self {
        Self(Arc::new(SnapshotLease {
            seq: db.snapshot_seq(),
            db_id: db.read.db_id,
            reader: Arc::downgrade(&db.read),
            snapshots: db.snapshot_list.clone(),
            sweep: db.dead_key_sweep.clone(),
        }))
    }

    pub fn sequence(&self) -> SequenceNumber {
        self.0.seq
    }

    pub fn read_options(&self) -> ReadOptions<'_> {
        ReadOptions::default().with_snapshot(self)
    }

    fn reader(&self) -> Result<Arc<ReadState>> {
        self.0.reader.upgrade().ok_or_else(Error::db_closed)
    }

    /// Copy the visible value at this snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.reader()?.point_lookup(&self.read_options(), key)
    }

    /// Pin the visible bytes at this snapshot. See [`PinnedSlice`] for retention costs.
    pub fn get_pinned(&self, key: &[u8]) -> Result<Option<PinnedSlice>> {
        self.reader()?.point_lookup(&self.read_options(), key)
    }

    /// Check point and range deletion visibility at this snapshot.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool> {
        Ok(self
            .reader()?
            .point_lookup::<usize>(&self.read_options(), key)?
            .is_some())
    }

    /// Iterate the visible rows in key order.
    pub fn iter(&self) -> Result<DBIterator> {
        self.iter_with_range(None, None)
    }

    /// Iterate the half-open range from the inclusive lower to the exclusive upper bound.
    pub fn iter_with_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<DBIterator> {
        self.reader()?
            .scan(&self.read_options(), lower, upper, None, None)
    }

    /// Iterate rows whose user keys begin with the supplied bytes.
    pub fn iter_with_prefix(&self, prefix: &[u8]) -> Result<DBIterator> {
        self.reader()?.scan(
            &self.read_options(),
            Some(prefix),
            types::prefix_successor(prefix).as_deref(),
            Some(prefix),
            None,
        )
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        self.db.release_snapshot(self.seq);
    }
}
