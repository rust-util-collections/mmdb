//! Per-DB background counters used by the L0 experiment.
use parking_lot::Mutex;

/// L0 I/O counters. Successfully built outputs may later be discarded at install.
#[derive(Clone, Copy, Debug, Default)]
pub struct L0CompactionProfile {
    /// Jobs whose range builds returned successfully and whose outputs were collected.
    pub rewrite_jobs: u64,
    /// Rewrite jobs with more than one target-level input file, including serial jobs.
    pub multiple_target_jobs: u64,
    /// Rewrite jobs that successfully joined more than one range build.
    pub parallel_jobs: u64,
    /// File relabels that completed the directory sync without rewriting data.
    pub trivial_moves: u64,
    /// Sum of selected input file lengths, excluding trivial moves.
    pub input_file_bytes: u64,
    /// Sum of newly built output file lengths.
    pub output_file_bytes: u64,
    /// Maximum of the opening L0 file count and later flush MANIFEST installs.
    pub peak_installed_l0_files: u64,
    /// All range entries, including a range that subsequently errored.
    pub ranges_started: u64,
    /// Highest number of overlapping range executions in this DB.
    pub peak_active_ranges: u64,
    active_ranges: u64,
}

pub(crate) struct ActiveRange<'a>(&'a Mutex<L0CompactionProfile>);

impl<'a> ActiveRange<'a> {
    pub(crate) fn enter(profile: &'a Mutex<L0CompactionProfile>) -> Self {
        let mut p = profile.lock();
        p.active_ranges += 1;
        p.ranges_started += 1;
        p.peak_active_ranges = p.peak_active_ranges.max(p.active_ranges);
        Self(profile)
    }
}

impl Drop for ActiveRange<'_> {
    fn drop(&mut self) {
        self.0.lock().active_ranges -= 1;
    }
}

#[cfg(test)]
mod tests {
    use crate::{DB, DbOptions};

    #[test]
    fn reopened_handle_starts_with_its_installed_l0_file_count() {
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions::default().with_l0_compaction_trigger(usize::MAX);
        let db = DB::open(options.clone(), dir.path()).unwrap();
        db.put(b"a", b"value").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
        let reopened = DB::open(options, dir.path()).unwrap();
        assert_eq!(
            reopened.get_property("num-files-at-level0").as_deref(),
            Some("1")
        );
        let profile = reopened.l0_compaction_profile();
        assert_eq!(profile.peak_installed_l0_files, 1);
        assert_eq!(profile.ranges_started, 0);
    }

    #[test]
    fn a_trivial_move_writes_zero_bytes_and_a_serial_multi_target_job_starts_one_range() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(
            DbOptions::default()
                .with_num_levels(2)
                .with_l0_compaction_trigger(usize::MAX),
            dir.path(),
        )
        .unwrap();
        db.put(b"a", b"first").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        let moved = db.l0_compaction_profile();
        assert_eq!(moved.trivial_moves, 1);
        assert_eq!(
            (
                moved.rewrite_jobs,
                moved.input_file_bytes,
                moved.output_file_bytes
            ),
            (0, 0, 0)
        );

        db.ingest_sorted([(b"z", b"second")]).unwrap();
        db.put(b"a", b"new").unwrap();
        db.put(b"z", b"new").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        let merged = db.l0_compaction_profile();
        assert_eq!(
            (
                merged.rewrite_jobs,
                merged.multiple_target_jobs,
                merged.parallel_jobs
            ),
            (1, 1, 0)
        );
        assert_eq!((merged.ranges_started, merged.peak_active_ranges), (1, 1));
        assert!(merged.input_file_bytes > 0 && merged.output_file_bytes > 0);
        assert_eq!(merged.trivial_moves, 1);
    }
}
