//! Per-DB background counters used by the L0 experiment.
use parking_lot::Mutex;

/// Rewrite work completed by L0 jobs, including outputs later discarded at install.
#[derive(Clone, Copy, Debug, Default)]
pub struct L0CompactionProfile {
    pub rewrite_jobs: u64,
    pub multiple_target_jobs: u64,
    pub parallel_jobs: u64,
    pub trivial_moves: u64,
    /// Sum of selected input file lengths, excluding trivial moves.
    pub input_file_bytes: u64,
    /// Sum of newly built output file lengths.
    pub output_file_bytes: u64,
    pub peak_installed_l0_files: u64,
    pub ranges_executed: u64,
    pub peak_active_ranges: u64,
    active_ranges: u64,
}

pub(crate) struct ActiveRange<'a>(&'a Mutex<L0CompactionProfile>);

impl<'a> ActiveRange<'a> {
    pub(crate) fn enter(profile: &'a Mutex<L0CompactionProfile>) -> Self {
        let mut p = profile.lock();
        p.active_ranges += 1;
        p.ranges_executed += 1;
        p.peak_active_ranges = p.peak_active_ranges.max(p.active_ranges);
        Self(profile)
    }
}

impl Drop for ActiveRange<'_> {
    fn drop(&mut self) {
        self.0.lock().active_ranges -= 1;
    }
}
