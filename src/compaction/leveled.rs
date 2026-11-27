//! Leveled compaction strategy.
//!
//! L0 → L1: merge all overlapping L0 files with overlapping L1 files.
//! Ln → Ln+1: pick a file from Ln, merge with overlapping files in Ln+1.
//!
//! After compaction, the old files are deleted and new files are installed.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::fs::{File, remove_file};
use std::path::Path;
use std::sync::{Arc, atomic::AtomicBool};
use std::thread::{self, scope};

use parking_lot::Mutex;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::cache::block_cache::BlockCache;
use crate::cache::table_cache::TableCache;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::merge::{IterSource, MergingIterator};
use crate::iterator::range_del::RangeTombstoneTracker;
use crate::manifest::file_numbers::{FileNumbers, PendingSsts};
use crate::manifest::version::{LevelFiles, TableFile, Version};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::manifest::version_set::{DirSynced, VersionSet, sync_new_sst_parent_dir};
use crate::options::{CompactionFilterDecision, DbOptions};
use crate::rate_limiter::RateLimiter;
use crate::sst::SstBuilds;
use crate::sst::range::RangeFragment;
use crate::sst::table_builder::{META_BLOCK_SPLIT_THRESHOLD, TableBuildOptions, TableBuilder};
use crate::sst::table_reader::TableIterator;
use crate::stats::DbStats;
use crate::types::{
    InternalKey, LazyValue, MAX_SEQUENCE_NUMBER, SequenceNumber, ValueType, compare_internal_key,
    decode_internal_key, tombstone_overlaps_bounds, user_key,
};

/// Test-only instrumentation: incremented each time `execute_compaction_io`
/// takes the parallel (`thread::scope`) sub-compaction path (`actual_subs > 1`),
/// so tests can directly prove real multi-threaded execution was exercised
/// rather than silently falling back to the always-available serial path.
#[cfg(test)]
static PARALLEL_SUB_COMPACTIONS_TAKEN: AtomicUsize = AtomicUsize::new(0);

/// A hint from the read path that a specific level may benefit from compaction.
/// Accumulated by the DB and drained by the compaction worker.
#[derive(Debug, Clone)]
pub struct CompactionHint {
    /// The level that was read-hot.
    pub level: usize,
}

/// Shared context for compaction operations, reducing argument count.
pub(crate) struct CompactionContext<'a> {
    pub db_path: &'a Path,
    pub table_cache: Option<&'a TableCache>,
    pub dir_sync: &'a Mutex<Option<File>>,
    pub manifest_poisoned: &'a AtomicBool,
    pub options: &'a DbOptions,
    pub builds: &'a SstBuilds,
    pub rate_limiter: Option<&'a Arc<RateLimiter>>,
    pub stats: Option<&'a Arc<DbStats>>,
    pub active_snapshots: &'a [SequenceNumber],
}

/// Description of a compaction to perform.
pub struct CompactionTask {
    /// Source level (files to compact from).
    pub level: usize,
    /// Destination level (`level + 1` for cross-level compactions, or `level`
    /// for an in-place bottommost rewrite).
    pub target_level: usize,
    /// Files from the source level.
    pub input_files_level: Vec<TableFile>,
    /// Files from the target level (empty for in-place bottommost rewrites).
    pub input_files_next: Vec<TableFile>,
    /// Unselected disjoint deeper levels (`L1+`), when `input_files_level` and
    /// `input_files_next` cover all overlapping files in `0..=target_level`.
    pub(crate) deeper_disjoint_levels: Option<Vec<Arc<LevelFiles>>>,
}

/// Range tombstone user-key extents `[begin, end)` written to each output
/// SST, keyed by file number. Captured from `TableBuildResult` during the
/// unlocked I/O phase so the install-phase overlap precheck never has to
/// open or read an SST while the caller holds the DB lock.
type OutputTombstones = HashMap<u64, Vec<(Vec<u8>, Vec<u8>)>>;

/// Result of the I/O phase of compaction (no lock needed to produce this).
pub struct CompactionOutput {
    dir_synced: DirSynced,
    tables: Vec<TableFile>,
    /// The version edit with new files added and old files deleted.
    pub edit: VersionEdit,
    /// Exact input files and levels the output was built from.
    pub input_files: Vec<(u32, FileMetaData)>,
    /// File numbers of all input files (for cache eviction and deletion).
    pub input_file_numbers: HashSet<u64>,
    /// Range tombstone extents per output file, for the install precheck.
    output_tombstones: OutputTombstones,
    /// True when the compaction filter removed or replaced a value. Install
    /// must discard this output if a snapshot was registered after pick.
    filter_mutated: bool,
}

/// Deferred cleanup actions that must be performed AFTER the manifest is
/// synced to disk. Executing these before sync risks data loss on crash.
pub struct PostCompactionCleanup {
    /// Old SST file numbers to delete from disk.
    pub files_to_delete: HashSet<u64>,
    /// True when `log_and_apply` installed the compaction edit (false when a
    /// stale/raced output was discarded).
    pub installed: bool,
}

impl CompactionTask {}

pub struct LeveledCompaction;

enum UserKeyRange {
    /// Inclusive file metadata range.
    File { smallest: Vec<u8>, largest: Vec<u8> },
    /// Half-open range tombstone extent.
    Tombstone { begin: Vec<u8>, end: Vec<u8> },
}

fn file_metadata_overlaps_bounds(
    tf: &TableFile,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> bool {
    if lower.zip(upper).is_some_and(|(lo, hi)| lo >= hi) {
        return false;
    }
    let file_smallest = user_key(&tf.meta.smallest_key);
    let file_largest = user_key(&tf.meta.largest_key);
    let above_lower = lower.is_none_or(|lo| file_largest >= lo);
    let below_upper = upper.is_none_or(|hi| file_smallest < hi);
    above_lower && below_upper
}

fn file_overlaps_compact_bounds(
    tf: &TableFile,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> bool {
    if file_metadata_overlaps_bounds(tf, lower, upper) {
        return true;
    }
    if !tf.meta.has_range_deletions {
        return false;
    }
    tf.reader
        .range_partition_extents()
        .any(|(begin, end)| tombstone_overlaps_bounds(begin, end, lower, upper))
}

fn add_file_extents(tf: &TableFile, extents: &mut Vec<UserKeyRange>) {
    if !tf.meta.smallest_key.is_empty() && !tf.meta.largest_key.is_empty() {
        extents.push(UserKeyRange::File {
            smallest: user_key(&tf.meta.smallest_key).to_vec(),
            largest: user_key(&tf.meta.largest_key).to_vec(),
        });
    }
    if tf.meta.has_range_deletions {
        extents.extend(tf.reader.range_partition_extents().map(|(begin, end)| {
            UserKeyRange::Tombstone {
                begin: begin.to_vec(),
                end: end.to_vec(),
            }
        }));
    }
}

fn file_metadata_overlaps_extent(tf: &TableFile, extent: &UserKeyRange) -> bool {
    let file_smallest = user_key(&tf.meta.smallest_key);
    let file_largest = user_key(&tf.meta.largest_key);
    match extent {
        UserKeyRange::File { smallest, largest } => {
            file_largest >= smallest.as_slice() && file_smallest <= largest.as_slice()
        }
        UserKeyRange::Tombstone { begin, end } => {
            file_largest >= begin.as_slice() && file_smallest < end.as_slice()
        }
    }
}

fn tombstone_overlaps_extent(begin: &[u8], end: &[u8], extent: &UserKeyRange) -> bool {
    match extent {
        UserKeyRange::File { smallest, largest } => {
            end > smallest.as_slice() && begin <= largest.as_slice()
        }
        UserKeyRange::Tombstone {
            begin: other_begin,
            end: other_end,
        } => end > other_begin.as_slice() && begin < other_end.as_slice(),
    }
}

fn file_overlaps_extent(tf: &TableFile, extent: &UserKeyRange) -> bool {
    if file_metadata_overlaps_extent(tf, extent) {
        return true;
    }
    if !tf.meta.has_range_deletions {
        return false;
    }
    tf.reader
        .range_partition_extents()
        .any(|(begin, end)| tombstone_overlaps_extent(begin, end, extent))
}

fn overlapping_files_for_inputs(files: &[TableFile], inputs: &[TableFile]) -> Vec<TableFile> {
    let mut extents = Vec::new();
    for tf in inputs {
        add_file_extents(tf, &mut extents);
    }

    // Seed with the aggregate [min smallest, max largest] range across ALL
    // inputs, not just per-file extents. The merged compaction output may span
    // key gaps between discontiguous inputs (output files are cut by size, not
    // at input-file boundaries), so a target-level file sitting inside such a
    // gap MUST be included as an input — otherwise the installed output would
    // overlap it, breaking the L1+ disjointness invariant and making keys
    // unreachable via binary search.
    {
        let mut agg_smallest: Option<&[u8]> = None;
        let mut agg_largest: Option<&[u8]> = None;
        for tf in inputs {
            if tf.meta.smallest_key.is_empty() || tf.meta.largest_key.is_empty() {
                continue;
            }
            let s = user_key(&tf.meta.smallest_key);
            let l = user_key(&tf.meta.largest_key);
            if agg_smallest.is_none_or(|cur| s < cur) {
                agg_smallest = Some(s);
            }
            if agg_largest.is_none_or(|cur| l > cur) {
                agg_largest = Some(l);
            }
        }
        if let (Some(s), Some(l)) = (agg_smallest, agg_largest) {
            extents.push(UserKeyRange::File {
                smallest: s.to_vec(),
                largest: l.to_vec(),
            });
        }
    }

    let mut selected_numbers = HashSet::new();
    loop {
        let mut changed = false;
        for tf in files {
            if selected_numbers.contains(&tf.meta.number) {
                continue;
            }
            if extents
                .iter()
                .any(|extent| file_overlaps_extent(tf, extent))
            {
                selected_numbers.insert(tf.meta.number);
                add_file_extents(tf, &mut extents);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // A tombstone can pull an earlier-key file in a later pass, so return the
    // selection in the level's own (key) order, not discovery order.
    files
        .iter()
        .filter(|tf| selected_numbers.contains(&tf.meta.number))
        .cloned()
        .collect()
}

/// Transitive point + range-tombstone extent closure over a sorted,
/// pairwise point-disjoint `L1+` slice (`files`). `range_indices` lists the
/// indices in `files` where `meta.has_range_deletions` is true.
fn overlapping_files_in_disjoint_level(
    files: &[TableFile],
    range_indices: &[usize],
    inputs: &[TableFile],
) -> Vec<TableFile> {
    if files.is_empty() || inputs.is_empty() {
        return Vec::new();
    }

    let inputs_have_ranges = inputs.iter().any(|tf| tf.meta.has_range_deletions);

    if range_indices.is_empty() && !inputs_have_ranges {
        let mut agg_smallest: Option<&[u8]> = None;
        let mut agg_largest: Option<&[u8]> = None;
        for tf in inputs {
            if tf.meta.smallest_key.is_empty() || tf.meta.largest_key.is_empty() {
                continue;
            }
            let s = user_key(&tf.meta.smallest_key);
            let l = user_key(&tf.meta.largest_key);
            if agg_smallest.is_none_or(|cur| s < cur) {
                agg_smallest = Some(s);
            }
            if agg_largest.is_none_or(|cur| l > cur) {
                agg_largest = Some(l);
            }
        }
        let Some((s, l)) = agg_smallest.zip(agg_largest) else {
            return Vec::new();
        };
        let lo = files.partition_point(|tf| {
            #[cfg(test)]
            PICKER_FILE_VISITS.with(|n| n.set(n.get() + 1));
            user_key(&tf.meta.largest_key) < s
        });
        let hi = lo
            + files[lo..].partition_point(|tf| {
                #[cfg(test)]
                PICKER_FILE_VISITS.with(|n| n.set(n.get() + 1));
                user_key(&tf.meta.smallest_key) <= l
            });
        return files[lo..hi].to_vec();
    }

    let mut pending_extents = Vec::new();
    for tf in inputs {
        add_file_extents(tf, &mut pending_extents);
    }
    {
        let mut agg_smallest: Option<&[u8]> = None;
        let mut agg_largest: Option<&[u8]> = None;
        for tf in inputs {
            if tf.meta.smallest_key.is_empty() || tf.meta.largest_key.is_empty() {
                continue;
            }
            let s = user_key(&tf.meta.smallest_key);
            let l = user_key(&tf.meta.largest_key);
            if agg_smallest.is_none_or(|cur| s < cur) {
                agg_smallest = Some(s);
            }
            if agg_largest.is_none_or(|cur| l > cur) {
                agg_largest = Some(l);
            }
        }
        if let (Some(s), Some(l)) = (agg_smallest, agg_largest) {
            pending_extents.push(UserKeyRange::File {
                smallest: s.to_vec(),
                largest: l.to_vec(),
            });
        }
    }

    let mut selected = std::collections::BTreeSet::<usize>::new();
    let mut remaining_ranges: Vec<usize> = range_indices.to_vec();
    let mut cursor = 0usize;

    while cursor < pending_extents.len() {
        let extent = match &pending_extents[cursor] {
            UserKeyRange::File { smallest, largest } => UserKeyRange::File {
                smallest: smallest.clone(),
                largest: largest.clone(),
            },
            UserKeyRange::Tombstone { begin, end } => UserKeyRange::Tombstone {
                begin: begin.clone(),
                end: end.clone(),
            },
        };
        cursor += 1;

        let (lo_key, inclusive_hi, hi_key): (&[u8], bool, &[u8]) = match &extent {
            UserKeyRange::File { smallest, largest } => (smallest, true, largest),
            UserKeyRange::Tombstone { begin, end } => (begin, false, end),
        };
        let lo = files.partition_point(|tf| {
            #[cfg(test)]
            PICKER_FILE_VISITS.with(|n| n.set(n.get() + 1));
            tf.meta.largest_key.is_empty() || user_key(&tf.meta.largest_key) < lo_key
        });
        let hi = lo
            + files[lo..].partition_point(|tf| {
                #[cfg(test)]
                PICKER_FILE_VISITS.with(|n| n.set(n.get() + 1));
                if tf.meta.smallest_key.is_empty() {
                    return true;
                }
                let s = user_key(&tf.meta.smallest_key);
                if inclusive_hi {
                    s <= hi_key
                } else {
                    s < hi_key
                }
            });

        let mut newly_added = Vec::new();
        for (offset, file) in files[lo..hi].iter().enumerate() {
            let idx = lo + offset;
            if !file.meta.smallest_key.is_empty()
                && !file.meta.largest_key.is_empty()
                && selected.insert(idx)
            {
                newly_added.push(idx);
            }
        }
        remaining_ranges.retain(|&idx| {
            if selected.contains(&idx) {
                return false;
            }
            #[cfg(test)]
            PICKER_FILE_VISITS.with(|n| n.set(n.get() + 1));
            if file_overlaps_extent(&files[idx], &extent) {
                selected.insert(idx);
                newly_added.push(idx);
                false
            } else {
                true
            }
        });

        for idx in newly_added {
            add_file_extents(&files[idx], &mut pending_extents);
        }
    }

    selected.into_iter().map(|idx| files[idx].clone()).collect()
}

/// A sub-task covering a key range within a compaction.
struct SubCompactionTask {
    /// Inclusive lower bound (user key). None = start from beginning.
    lower_bound: Option<Vec<u8>>,
    /// Exclusive upper bound (user key). None = go to end.
    upper_bound: Option<Vec<u8>>,
    /// Files from the source level that overlap this range.
    input_files_level: Vec<TableFile>,
    /// Files from the target level within this range.
    input_files_next: Vec<TableFile>,
}

/// Parameters shared across all sub-compactions within a single compaction I/O.
struct SubCompactionParams<'a> {
    target_level: usize,
    is_bottommost: bool,
    deeper_disjoint_levels: Option<&'a [Arc<LevelFiles>]>,
    build_opts: &'a TableBuildOptions,
    oldest_snapshot_seq: SequenceNumber,
    pending: &'a PendingSsts<'a>,
    range_fragments: &'a [RangeFragment],
}

fn key_exists_in_disjoint_levels(
    levels: &[Arc<LevelFiles>],
    target_user_key: &[u8],
) -> Result<bool> {
    for files in levels {
        let end = files.partition_point(|f| user_key(&f.meta.smallest_key) <= target_user_key);
        for file in &files[end.saturating_sub(1)..end] {
            if target_user_key < user_key(&file.meta.smallest_key)
                || target_user_key > user_key(&file.meta.largest_key)
            {
                continue;
            }
            if file
                .reader
                .point_value::<usize>(target_user_key, MAX_SEQUENCE_NUMBER, false)
                .ctx()?
                .is_some()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Output of a single sub-compaction (new files only; deletions handled by orchestrator).
struct SubCompactionOutput {
    new_files: Vec<(u32, FileMetaData)>,
    /// Range tombstone extents per output file (see `OutputTombstones`).
    output_tombstones: OutputTombstones,
    filter_mutated: bool,
}

/// Compute split points from target-level file boundaries.
/// Returns user keys that divide the key space into sub-ranges.
fn compute_split_points(task: &CompactionTask, max_subs: usize) -> Vec<Vec<u8>> {
    if max_subs <= 1 {
        return Vec::new();
    }
    let next_files = &task.input_files_next;
    if next_files.len() <= 1 {
        return Vec::new(); // Not enough target files to split on
    }

    // Extract user key boundaries from target-level files (all except the last,
    // since the last file's largest_key doesn't split anything).
    let mut boundaries: Vec<Vec<u8>> = next_files[..next_files.len() - 1]
        .iter()
        .map(|tf| user_key(&tf.meta.largest_key).to_vec())
        .collect();
    // Sub-job ranges `[split[i-1], split[i])` must be increasing and disjoint.
    boundaries.sort();
    boundaries.dedup();

    if boundaries.is_empty() {
        return Vec::new();
    }

    let desired_splits = (max_subs - 1).min(boundaries.len());
    if desired_splits >= boundaries.len() {
        return boundaries;
    }

    // Select evenly-spaced boundaries
    let mut splits = Vec::with_capacity(desired_splits);
    for i in 1..=desired_splits {
        let idx = i * boundaries.len() / (desired_splits + 1);
        splits.push(boundaries[idx.min(boundaries.len() - 1)].clone());
    }
    splits
}

/// Build sub-tasks from a compaction task and split points.
fn build_sub_tasks(task: &CompactionTask, split_points: &[Vec<u8>]) -> Vec<SubCompactionTask> {
    let n = split_points.len() + 1;
    let mut sub_tasks = Vec::with_capacity(n);

    let next_files = &task.input_files_next;

    for i in 0..n {
        let lower = if i == 0 {
            None
        } else {
            Some(split_points[i - 1].clone())
        };
        let upper = if i == n - 1 {
            None
        } else {
            Some(split_points[i].clone())
        };

        // Filter target-level files to those overlapping [lower, upper)
        let sub_next: Vec<TableFile> = next_files
            .iter()
            .filter(|tf| {
                let file_smallest = user_key(&tf.meta.smallest_key);
                let file_largest = user_key(&tf.meta.largest_key);
                let above_lower = match &lower {
                    Some(lo) => file_largest >= lo.as_slice(),
                    None => true,
                };
                let below_upper = match &upper {
                    Some(hi) => file_smallest < hi.as_slice(),
                    None => true,
                };
                above_lower && below_upper
            })
            .cloned()
            .collect();

        // For L0: all source files (they overlap arbitrarily).
        // For Ln: only files overlapping this sub-range.
        let sub_level: Vec<TableFile> = if task.level == 0 {
            task.input_files_level.clone()
        } else {
            task.input_files_level
                .iter()
                .filter(|tf| {
                    let file_smallest = user_key(&tf.meta.smallest_key);
                    let file_largest = user_key(&tf.meta.largest_key);
                    let above_lower = match &lower {
                        Some(lo) => file_largest >= lo.as_slice(),
                        None => true,
                    };
                    let below_upper = match &upper {
                        Some(hi) => file_smallest < hi.as_slice(),
                        None => true,
                    };
                    above_lower && below_upper
                })
                .cloned()
                .collect()
        };

        sub_tasks.push(SubCompactionTask {
            lower_bound: lower,
            upper_bound: upper,
            input_files_level: sub_level,
            input_files_next: sub_next,
        });
    }

    sub_tasks
}

/// Collect raw tombstone triples from input files.
enum OutputEntry {
    Point(Vec<u8>, LazyValue),
    Range(RangeFragment),
}

impl OutputEntry {
    fn user_key(&self) -> &[u8] {
        match self {
            Self::Point(key, _) => user_key(key),
            Self::Range(range) => &range.begin,
        }
    }
}

fn cleanup_output_files(db_path: &Path, new_files: &[(u32, FileMetaData)]) {
    for (_, meta) in new_files {
        let orphan = db_path.join(format!("{:06}.sst", meta.number));
        let _ = remove_file(&orphan);
    }
}

fn evict_table_cache_files(table_cache: Option<&Arc<TableCache>>, files: &[(u32, FileMetaData)]) {
    if let Some(cache) = table_cache {
        for (_, meta) in files {
            cache.evict(meta.number);
        }
    }
}

fn collect_range_fragments(files: &[TableFile]) -> Result<Vec<RangeFragment>> {
    let mut tombstones = Vec::new();
    for tf in files {
        if tf.meta.has_range_deletions {
            tf.reader.collect_range_fragments(&mut tombstones).ctx()?;
        }
    }
    Ok(tombstones)
}

/// Execute a single sub-compaction covering [lower_bound, upper_bound).
/// File numbers are allocated from a shared atomic counter to avoid collisions.
/// Range deletions are shared across sub-tasks, clipped to the owned interval
/// for output and applied to all covered point versions during GC.
fn execute_sub_compaction_io(
    ctx: &CompactionContext<'_>,
    sub: &SubCompactionTask,
    params: &SubCompactionParams<'_>,
) -> Result<SubCompactionOutput> {
    // Build streaming merge sources
    let mut sources: Vec<IterSource> = Vec::new();
    for tf in &sub.input_files_level {
        // Compaction scans every input block exactly once; filling the
        // block cache would evict hot point-read blocks for no benefit.
        let iter = TableIterator::new(tf.reader.clone()).with_fill_cache(false);
        // from_table_iter (not from_boxed): surfaces iter_error so a mid-block
        // decode/CRC failure fails the job instead of looking like clean EOF.
        sources.push(IterSource::from_table_iter(iter));
    }
    for tf in &sub.input_files_next {
        // Compaction scans every input block exactly once; filling the
        // block cache would evict hot point-read blocks for no benefit.
        let iter = TableIterator::new(tf.reader.clone()).with_fill_cache(false);
        sources.push(IterSource::from_table_iter(iter));
    }

    // Clip each interval once. The newest stripe visible at the oldest
    // snapshot suffices for the point-GC tracker; newer stripes stay packed.
    let mut clipped = Vec::new();
    let mut range_tombstones = RangeTombstoneTracker::new(params.oldest_snapshot_seq);
    let lo = sub.lower_bound.as_deref().map(Arc::<[u8]>::from);
    let hi = sub.upper_bound.as_deref().map(Arc::<[u8]>::from);
    for input in params.range_fragments {
        if !tombstone_overlaps_bounds(&input.begin, &input.end, lo.as_deref(), hi.as_deref()) {
            continue;
        }
        let mut range = input.clone();
        if let Some(lo) = &lo
            && range.begin < *lo
        {
            range.begin = lo.clone();
        }
        if let Some(hi) = &hi
            && range.end > *hi
        {
            range.end = hi.clone();
        }
        let visible = range
            .sequences()
            .partition_point(|s| *s <= params.oldest_snapshot_seq);
        if visible > 0 {
            range_tombstones.add(
                range.begin.to_vec(),
                range.end.to_vec(),
                range.sequences()[visible - 1],
            );
        }
        if !params.is_bottommost || range.keep_from(params.oldest_snapshot_seq) {
            clipped.push(range);
        }
    }
    range_tombstones.reset();
    clipped.sort_unstable_by(|a, b| a.begin.cmp(&b.begin));
    let mut ranges = clipped.into_iter().peekable();

    let mut merger = MergingIterator::new(sources, compare_internal_key);

    // If lower_bound is set, seek to the first internal key of that user
    // key. RangeDeletion sorts before Value at the same sequence, so a
    // tombstone at MAX_SEQUENCE_NUMBER is included. The previous sub-job
    // already stopped at this user key and did not emit it.
    if let Some(ref lo) = sub.lower_bound {
        let seek_key = InternalKey::new(lo, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion)
            .as_bytes()
            .to_vec();
        merger.seek(&seek_key);
    }

    let mut new_files: Vec<(u32, FileMetaData)> = Vec::new();
    let mut output_tombstones = OutputTombstones::new();
    let mut builder: Option<TableBuilder> = None;
    let mut current_file_number = 0u64;
    let mut current_size = 0usize;
    let mut current_file_user_key: Vec<u8> = Vec::new();
    let mut carry = Vec::new();
    let mut pending_cut = false;
    let mut last_point_key: Option<Vec<u8>> = None;
    let mut prev_raw_trailer: u64 = 0;
    let mut last_written_seq: SequenceNumber = 0;
    let mut snapshot_idx: usize = ctx.active_snapshots.len();
    let mut filter_mutated = false;
    let mut rate_limit_pending = 0usize;
    const RATE_LIMIT_BATCH_BYTES: usize = 64 * 1024;
    let mut next_point = merger.next_entry();
    loop {
        let mut row = if ranges.peek().is_some_and(|r| {
            next_point
                .as_ref()
                .is_none_or(|(key, _)| r.begin.as_ref() <= user_key(key))
        }) {
            OutputEntry::Range(ranges.next().unwrap())
        } else if let Some((key, value)) = next_point.take() {
            if hi
                .as_ref()
                .is_some_and(|bound| user_key(&key) >= bound.as_ref())
            {
                break;
            }
            next_point = merger.next_entry();
            OutputEntry::Point(key, value)
        } else {
            break;
        };
        if let OutputEntry::Point(ikey, final_value) = &mut row {
            let (user_key, entry_seq, vt) = decode_internal_key(ikey).ctx()?;
            if vt == ValueType::RangeDeletion {
                return Err(Error::corruption("range deletion in point data block"));
            }
            let cur_trailer = u64::from_be_bytes(ikey[ikey.len() - 8..].try_into().unwrap());
            let same_user_key = if let Some(ref prev_uk) = last_point_key {
                match prev_uk.as_slice().cmp(user_key) {
                    CmpOrdering::Greater => {
                        return Err(Error::corruption("unordered point keys in SST data block"));
                    }
                    CmpOrdering::Equal => {
                        if prev_raw_trailer > cur_trailer {
                            return Err(Error::corruption(
                                "unordered point keys in SST data block",
                            ));
                        }
                        true
                    }
                    CmpOrdering::Less => false,
                }
            } else {
                if sub.lower_bound.as_deref().is_some_and(|lo| user_key < lo) {
                    return Err(Error::corruption("unordered point keys in SST data block"));
                }
                false
            };
            prev_raw_trailer = cur_trailer;
            if same_user_key {
                while snapshot_idx > 0 && ctx.active_snapshots[snapshot_idx - 1] >= last_written_seq
                {
                    snapshot_idx -= 1;
                }
                if snapshot_idx > 0 && ctx.active_snapshots[snapshot_idx - 1] >= entry_seq {
                    last_written_seq = entry_seq;
                    // A retained older version that is shadowed by a range tombstone
                    // below the oldest snapshot must still be dropped. Otherwise, if
                    // that tombstone is itself dropped at the bottommost level, the
                    // value would resurrect for the snapshot that retained it. (The
                    // newest-version branch below already applies this check; retained
                    // versions need it too.)
                    if vt == ValueType::Value
                        && !range_tombstones.is_empty()
                        && range_tombstones.is_deleted(user_key, entry_seq)
                    {
                        continue;
                    }
                } else {
                    continue;
                }
            } else {
                let buf = last_point_key.get_or_insert_with(Vec::new);
                buf.clear();
                buf.extend_from_slice(user_key);
                last_written_seq = entry_seq;
                snapshot_idx = ctx.active_snapshots.len();

                // Only a genuine Deletion may be dropped here, and only at the
                // bottommost level below the oldest snapshot. Type was validated
                // by `decode_internal_key` above.
                if params.is_bottommost
                    && entry_seq < params.oldest_snapshot_seq
                    && vt == ValueType::Deletion
                {
                    continue;
                }

                if vt == ValueType::Value
                    && !range_tombstones.is_empty()
                    && range_tombstones.is_deleted(user_key, entry_seq)
                {
                    continue;
                }
            }

            // Apply compaction filter when bottommost across the entire file range,
            // or per-key bottommost when no deeper unselected file contains `user_key`.
            if (params.is_bottommost || params.deeper_disjoint_levels.is_some())
                && ctx.active_snapshots.is_empty()
                && let Some(ref filter) = ctx.options.compaction_filter
                && vt == ValueType::Value
            {
                match filter.filter(params.target_level, user_key, final_value.as_slice()) {
                    CompactionFilterDecision::Keep => {}
                    CompactionFilterDecision::Remove => {
                        let key_bottommost = params.is_bottommost
                            || !key_exists_in_disjoint_levels(
                                params.deeper_disjoint_levels.unwrap(),
                                user_key,
                            )?;
                        if key_bottommost {
                            filter_mutated = true;
                            continue;
                        }
                    }
                    CompactionFilterDecision::ChangeValue(new_val) => {
                        let key_bottommost = params.is_bottommost
                            || !key_exists_in_disjoint_levels(
                                params.deeper_disjoint_levels.unwrap(),
                                user_key,
                            )?;
                        if key_bottommost {
                            filter_mutated = true;
                            *final_value = LazyValue::Inline(new_val);
                        }
                    }
                }
            }

            if params.is_bottommost
                && entry_seq > 0
                && entry_seq < params.oldest_snapshot_seq
                && vt == ValueType::Value
            {
                *ikey = InternalKey::new(user_key, 0, vt).into_bytes();
            }
        }

        let user_key = row.user_key();
        // Only cut output files at a user-key boundary: all versions of one user
        // key must stay in the same file, otherwise L1+ files would have
        // overlapping key ranges and a point read could pick the wrong file and
        // miss a visible version. A size-triggered cut is deferred until the user
        // key changes here.
        if pending_cut && builder.is_some() && user_key != current_file_user_key.as_slice() {
            let mut previous = builder.take().unwrap();
            carry = previous.clip_at(user_key);
            let result = previous.finish().ctx()?;
            if let Some(s) = ctx.stats {
                s.record_compaction_bytes(result.file_size);
            }
            if result.has_range_deletions {
                output_tombstones.insert(current_file_number, result.range_tombstones);
            }
            new_files.push((
                params.target_level as u32,
                FileMetaData {
                    number: current_file_number,
                    file_size: result.file_size,
                    smallest_key: result.smallest_key.unwrap_or_default(),
                    largest_key: result.largest_key.unwrap_or_default(),
                    has_range_deletions: result.has_range_deletions,
                },
            ));
            current_size = 0;
            pending_cut = false;
        }

        // Create new output file if needed
        if builder.is_none() {
            current_file_number = params.pending.allocate().ctx()?;
            let sst_path = ctx.db_path.join(format!("{:06}.sst", current_file_number));
            let mut opts = params.build_opts.clone();
            opts.block_property_collectors = ctx
                .options
                .block_property_collectors
                .iter()
                .map(|f| f())
                .collect();
            let mut output = TableBuilder::new(&sst_path, opts).ctx()?;
            output.start_output(ctx.active_snapshots, std::mem::take(&mut carry));
            builder = Some(output);
            current_size = 0;
        }

        if current_file_user_key != user_key {
            current_file_user_key.clear();
            current_file_user_key.extend_from_slice(user_key);
        }
        let output = builder.as_mut().unwrap();
        let entry_bytes = match row {
            OutputEntry::Point(key, value) => {
                output.add(&key, value.as_slice()).ctx()?;
                key.len() + value.len()
            }
            OutputEntry::Range(range) => {
                let bytes = range.begin.len() + range.end.len() + size_of_val(range.sequences());
                output.add_range_fragment(range);
                bytes
            }
        };
        current_size = current_size.saturating_add(entry_bytes);

        if let Some(rl) = ctx.rate_limiter {
            rate_limit_pending = rate_limit_pending.saturating_add(entry_bytes);
            if rate_limit_pending >= RATE_LIMIT_BATCH_BYTES {
                rl.request(rate_limit_pending);
                rate_limit_pending = 0;
            }
        }

        if current_size >= ctx.options.target_file_size_base as usize
            || builder.as_ref().unwrap().projected_meta_size() >= META_BLOCK_SPLIT_THRESHOLD
        {
            // Defer the actual file cut to the next user-key boundary (handled at
            // the top of the loop) so a key's versions are never split across files.
            // The meta-size condition keeps each single-block metadata structure
            // well below the reader's hard cap for key-heavy data.
            pending_cut = true;
        }
    }

    if rate_limit_pending > 0
        && let Some(rl) = ctx.rate_limiter
    {
        rl.request(rate_limit_pending);
    }

    // Flush remaining builder
    if let Some(b) = builder {
        let result = b.finish().ctx()?;
        if let Some(s) = ctx.stats {
            s.record_compaction_bytes(result.file_size);
        }
        if result.has_range_deletions {
            output_tombstones.insert(current_file_number, result.range_tombstones);
        }
        new_files.push((
            params.target_level as u32,
            FileMetaData {
                number: current_file_number,
                file_size: result.file_size,
                smallest_key: result.smallest_key.unwrap_or_default(),
                largest_key: result.largest_key.unwrap_or_default(),
                has_range_deletions: result.has_range_deletions,
            },
        ));
    }

    if let Some(e) = merger.error() {
        return Err(e.clone().context("sub-compaction merge error"));
    }

    Ok(SubCompactionOutput {
        new_files,
        output_tombstones,
        filter_mutated,
    })
}

impl LeveledCompaction {
    fn level_over_budget(version: &Version, options: &DbOptions, level: usize) -> bool {
        let files = version.level_files(level);
        if files.is_empty() {
            return false;
        }
        let size: u64 = files
            .iter()
            .map(|file| file.meta.file_size)
            .fold(0, u64::saturating_add);
        size > 0 && size > Self::max_bytes_for_level(options, level)
    }

    /// Informational backlog test without scheduling work or opening input blocks.
    pub fn is_pending(version: &Version, options: &DbOptions) -> bool {
        let l0 = version.l0_file_count();
        (l0 > 0 && l0 >= options.l0_compaction_trigger)
            || (1..version.num_levels - 1)
                .any(|level| Self::level_over_budget(version, options, level))
    }

    /// Check if compaction is needed and return a task if so.
    ///
    /// `in_flight` excludes file numbers already claimed by another
    /// concurrent compaction's pick (see `DB::compacting_files`) — with
    /// `max_background_compactions > 1`, multiple threads call this
    /// independently, and without exclusion two threads could pick the same
    /// input files and redundantly re-process them before
    /// `install_compaction`'s stale-input check discards the loser. Pass an
    /// empty set for read-only "is compaction pending"-style checks that
    /// don't actually claim the result.
    pub fn pick_compaction(
        version: &Version,
        options: &DbOptions,
        in_flight: &HashSet<u64>,
    ) -> Option<CompactionTask> {
        // Prioritize L0 → L1 whenever L0 reaches `l0_compaction_trigger` so
        // overlapping flushes coalesce into L1 before deeper levels rewrite.
        // If L0 is already claimed by another concurrent worker, fall through
        // to deeper levels ordered by byte-budget score.
        let l0_count = version.l0_file_count();
        if l0_count > 0
            && l0_count >= options.l0_compaction_trigger
            && let Some(picked) = Self::pick_l0_compaction(version, in_flight)
        {
            return Some(picked);
        }

        let mut candidates: smallvec::SmallVec<[(f64, usize); 8]> = smallvec::SmallVec::new();
        for level in 1..version.num_levels.saturating_sub(1) {
            let files = version.level_files(level);
            if files.is_empty() {
                continue;
            }
            let size: u64 = files
                .iter()
                .map(|file| file.meta.file_size)
                .fold(0, u64::saturating_add);
            if size == 0 {
                continue;
            }
            let max_bytes = Self::max_bytes_for_level(options, level);
            if size > max_bytes {
                let score = if max_bytes == 0 {
                    f64::INFINITY
                } else {
                    size as f64 / max_bytes as f64
                };
                candidates.push((score, level));
            }
        }

        // Highest byte-budget score wins; ties prefer shallower levels.
        candidates.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

        for (_, level) in candidates {
            if let Some(picked) = Self::pick_level_compaction(version, level, in_flight) {
                return Some(picked);
            }
        }

        None
    }

    /// Pick L0 → L1 compaction. Returns `None` (deferring to another
    /// priority) if any current L0 file is already claimed by another
    /// in-flight compaction — L0 compaction always includes the *entire*
    /// current L0 file set, so a partial pick isn't meaningful here.
    /// Opportunistic callers treat that as nothing-to-do (the in-flight
    /// compaction is already draining L0), while the explicit full-compaction
    /// path waits for the claim to clear and re-picks (`DB::drain_l0` with
    /// `wait_for_inflight`).
    pub(crate) fn pick_l0_compaction(
        version: &Version,
        in_flight: &HashSet<u64>,
    ) -> Option<CompactionTask> {
        let l0_files = version.level_files(0);
        if l0_files.is_empty()
            || l0_files
                .iter()
                .any(|tf| in_flight.contains(&tf.meta.number))
        {
            return None;
        }

        // All L0 files participate (they may overlap with each other)
        let input_l0: Vec<TableFile> = l0_files.to_vec();

        // A range tombstone's end key is not in SST smallest/largest metadata.
        // Metadata-only overlap misses an L1 tombstone whose begin key sits
        // outside the L0 span; install then discards the output and this same
        // pick repeats. Close L1 over point and tombstone extents, matching
        // `pick_level_compaction` and `pick_compaction_for_range`.
        let l1_files = version.level_files(1);
        let l1_range_indices: Vec<usize> = l1_files
            .iter()
            .enumerate()
            .filter_map(|(i, tf)| tf.meta.has_range_deletions.then_some(i))
            .collect();
        let input_l1 = overlapping_files_in_disjoint_level(l1_files, &l1_range_indices, &input_l0);
        if input_l1
            .iter()
            .any(|tf| in_flight.contains(&tf.meta.number))
        {
            return None;
        }

        Some(CompactionTask {
            level: 0,
            target_level: 1,
            input_files_level: input_l0,
            input_files_next: input_l1,
            deeper_disjoint_levels: Some(version.files[2.min(version.num_levels)..].to_vec()),
        })
    }

    /// Pick Ln → Ln+1 compaction by minimum next-level overlap ratio
    /// (`kMinOverlappingRatio`, tie-breaking toward single-source trivial-move
    /// eligibility and larger source files), skipping candidates whose
    /// transitive extent closure touches `in_flight`.
    fn pick_level_compaction(
        version: &Version,
        level: usize,
        in_flight: &HashSet<u64>,
    ) -> Option<CompactionTask> {
        let files = version.level_files(level);
        if files.is_empty() {
            return None;
        }

        struct LevelCandidate {
            next_bytes: u128,
            src_bytes: u128,
            single_source: bool,
            target_size: u64,
            input_files_level: Vec<TableFile>,
            input_files_next: Vec<TableFile>,
        }

        let next_level = level + 1;
        let next_files: &[TableFile] = if next_level < version.num_levels {
            version.level_files(next_level)
        } else {
            &[]
        };
        let src_range_indices: Vec<usize> = files
            .iter()
            .enumerate()
            .filter_map(|(i, tf)| tf.meta.has_range_deletions.then_some(i))
            .collect();
        let next_range_indices: Vec<usize> = next_files
            .iter()
            .enumerate()
            .filter_map(|(i, tf)| tf.meta.has_range_deletions.then_some(i))
            .collect();
        let mut evaluated_multi: HashSet<u64> = HashSet::new();
        let mut best: Option<LevelCandidate> = None;

        for target in files {
            #[cfg(test)]
            PICKER_FILE_VISITS.with(|n| n.set(n.get() + 1));
            if in_flight.contains(&target.meta.number)
                || evaluated_multi.contains(&target.meta.number)
            {
                continue;
            }
            let input_level = if src_range_indices.is_empty() && !target.meta.has_range_deletions {
                vec![target.clone()]
            } else {
                overlapping_files_in_disjoint_level(
                    files,
                    &src_range_indices,
                    std::slice::from_ref(target),
                )
            };
            if input_level.len() > 1 {
                evaluated_multi.extend(input_level.iter().map(|tf| tf.meta.number));
            }
            if input_level
                .iter()
                .any(|tf| in_flight.contains(&tf.meta.number))
            {
                continue;
            }

            let input_next = if !next_files.is_empty() {
                overlapping_files_in_disjoint_level(next_files, &next_range_indices, &input_level)
            } else {
                Vec::new()
            };
            if input_next
                .iter()
                .any(|tf| in_flight.contains(&tf.meta.number))
            {
                continue;
            }

            let src_bytes: u128 = input_level
                .iter()
                .map(|tf| tf.meta.file_size as u128)
                .sum::<u128>()
                .max(1);
            let next_bytes: u128 = input_next.iter().map(|tf| tf.meta.file_size as u128).sum();
            let single_source = input_level.len() == 1;
            let target_size = input_level
                .iter()
                .map(|tf| tf.meta.file_size)
                .max()
                .unwrap_or(target.meta.file_size);

            let better = match &best {
                None => true,
                Some(cur) => {
                    match (next_bytes * cur.src_bytes).cmp(&(cur.next_bytes * src_bytes)) {
                        CmpOrdering::Less => true,
                        CmpOrdering::Equal => {
                            (single_source, target_size) > (cur.single_source, cur.target_size)
                        }
                        CmpOrdering::Greater => false,
                    }
                }
            };
            if better {
                best = Some(LevelCandidate {
                    next_bytes,
                    src_bytes,
                    single_source,
                    target_size,
                    input_files_level: input_level,
                    input_files_next: input_next,
                });
            }
        }

        let picked = best?;
        Some(CompactionTask {
            level,
            target_level: next_level,
            input_files_level: picked.input_files_level,
            input_files_next: picked.input_files_next,
            deeper_disjoint_levels: Some(
                version.files[(next_level + 1).min(version.num_levels)..].to_vec(),
            ),
        })
    }

    /// Pick compaction for a specific key range. Collects files overlapping
    /// `[begin, end)` at each level and compacts them, ending with an in-place
    /// bottommost pass at `num_levels - 1` for overlapping files not yet
    /// rewritten by the current `compact_range` call (`settled_bottom`).
    pub(crate) fn pick_compaction_for_range(
        version: &Version,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
        settled_bottom: &HashSet<u64>,
    ) -> Option<CompactionTask> {
        if begin.zip(end).is_some_and(|(b, e)| b >= e) {
            return None;
        }
        // Check L0 first: collect L0 files overlapping the range
        let l0_files = version.level_files(0);
        let mut input_l0: Vec<TableFile> = Vec::new();
        for tf in l0_files {
            if file_overlaps_compact_bounds(tf, begin, end) {
                input_l0.push(tf.clone());
            }
        }

        if !input_l0.is_empty() {
            // A range tombstone's end key is not represented by the file's
            // smallest/largest metadata, so the bounds filter above can pick
            // a tombstone whose real reach extends past [begin, end) without
            // pulling in a same-level sibling that holds a point covered
            // only by that wider reach. Close the source level transitively
            // over real point and tombstone extents — same as
            // `pick_level_compaction` — before deriving the target-level
            // inputs. Otherwise a lone selected tombstone can trivial-move
            // to L1 while the point it covers stays behind at L0, and
            // level-aware tombstone lookups (which only let a tombstone
            // cover keys at its own level or shallower) resurrect it.
            input_l0 = overlapping_files_for_inputs(l0_files, &input_l0);
            let input_l1 = overlapping_files_for_inputs(version.level_files(1), &input_l0);
            return Some(CompactionTask {
                level: 0,
                target_level: 1,
                input_files_level: input_l0,
                input_files_next: input_l1,
                deeper_disjoint_levels: None,
            });
        }

        // Check L1..num_levels-1 cross-level compactions
        for level in 1..version.num_levels - 1 {
            let files = version.level_files(level);
            let mut input_level: Vec<TableFile> = Vec::new();
            for tf in files {
                if file_overlaps_compact_bounds(tf, begin, end) {
                    input_level.push(tf.clone());
                }
            }

            if !input_level.is_empty() {
                // Same source-level closure as L0 above: a selected file's
                // tombstone reach can cover a same-level sibling's point
                // that the bounds filter alone would leave behind.
                input_level = overlapping_files_for_inputs(files, &input_level);
                let next_level = level + 1;
                let input_next =
                    overlapping_files_for_inputs(version.level_files(next_level), &input_level);
                return Some(CompactionTask {
                    level,
                    target_level: next_level,
                    input_files_level: input_level,
                    input_files_next: input_next,
                    deeper_disjoint_levels: None,
                });
            }
        }

        // Bottommost level (`num_levels - 1`): once shallower levels are clear,
        // rewrite overlapping bottommost files once so CompactionFilter,
        // lazy_delete, and snapshot-expired tombstones/versions take effect
        // even when data already lives at `num_levels - 1` (or reached it via
        // trivial moves).
        let bottom = version.num_levels - 1;
        let bottom_files = version.level_files(bottom);
        let mut input_bottom: Vec<TableFile> = bottom_files
            .iter()
            .filter(|tf| file_overlaps_compact_bounds(tf, begin, end))
            .cloned()
            .collect();
        if !input_bottom.is_empty() {
            input_bottom = overlapping_files_for_inputs(bottom_files, &input_bottom);
            if input_bottom
                .iter()
                .any(|tf| !settled_bottom.contains(&tf.meta.number))
            {
                return Some(CompactionTask {
                    level: bottom,
                    target_level: bottom,
                    input_files_level: input_bottom,
                    input_files_next: Vec::new(),
                    deeper_disjoint_levels: None,
                });
            }
        }

        None
    }

    /// Pure I/O phase of compaction — does NOT require any lock.
    ///
    /// Merges input files, writes output SSTs using the shared file-number allocator,
    /// and returns a `CompactionOutput` ready for `install_compaction`.
    ///
    /// When `options.max_subcompactions > 1` and the target level has enough
    /// files to split on, the work is divided into parallel sub-compactions
    /// using `std::thread::scope`.
    pub(crate) fn execute_compaction_io(
        ctx: &CompactionContext<'_>,
        task: &CompactionTask,
        file_numbers: &FileNumbers,
        is_bottommost: bool,
    ) -> Result<CompactionOutput> {
        let _build = ctx.builds.enter();
        let target_level = task.target_level;

        // Trivial move optimization: if there's exactly one input file and no
        // overlap with the next level, just move the metadata without
        // rewriting. Skip it for an in-place bottommost rewrite
        // (`target_level == task.level`) and when the compaction filter could
        // remove or change entries — it needs to see every key-value pair. A
        // filter that reports itself a no-op (e.g. lazy-delete with no user
        // filter and no registered dead keys) still permits cross-level moves.
        // Building this as a normal `CompactionOutput` (rather than applying
        // it directly) means it goes through `install_compaction`'s
        // stale-input check like any other compaction — required now that
        // every caller of this function (including concurrent background
        // workers) can reach this path, not just the single-threaded
        // `do_compaction`.
        if target_level != task.level
            && task.input_files_level.len() == 1
            && task.input_files_next.is_empty()
            && ctx
                .options
                .compaction_filter
                .as_ref()
                .is_none_or(|f| f.is_noop())
        {
            let tf = &task.input_files_level[0];
            let dir_synced = sync_new_sst_parent_dir(ctx.dir_sync, ctx.manifest_poisoned)?;
            #[cfg(feature = "bench-metrics")]
            if task.level == 0
                && let Some(stats) = ctx.stats
            {
                stats.l0_profile.lock().trivial_moves += 1;
            }
            let mut edit = VersionEdit::new();
            edit.delete_file(task.level as u32, tf.meta.number);
            edit.add_file(target_level as u32, tf.meta.clone());
            return Ok(CompactionOutput {
                dir_synced,
                tables: vec![tf.clone()],
                edit,
                input_files: vec![(task.level as u32, tf.meta.clone())],
                // Empty, NOT `{tf.meta.number}`: a trivial move relabels the
                // same physical file to a new level rather than replacing it
                // with a newly-written one. `input_file_numbers` drives both
                // cache eviction and (via `PostCompactionCleanup`) physical
                // deletion in `install_compaction` — including this file's
                // number here would delete the very file this edit just
                // added back into the version.
                input_file_numbers: HashSet::new(),
                output_tombstones: OutputTombstones::new(),
                filter_mutated: false,
            });
        }

        let oldest_snapshot_seq = ctx
            .active_snapshots
            .iter()
            .min()
            .copied()
            .unwrap_or(SequenceNumber::MAX);

        let target_compression = if !ctx.options.compression_per_level.is_empty()
            && target_level < ctx.options.compression_per_level.len()
        {
            ctx.options.compression_per_level[target_level]
        } else {
            ctx.options.compression
        };
        let build_opts = TableBuildOptions {
            block_size: ctx.options.block_size,
            block_restart_interval: ctx.options.block_restart_interval,
            bloom_bits_per_key: ctx.options.bloom_bits_for_level(target_level),
            prefix_bloom_bits_per_key: Some(ctx.options.bloom_bits_per_key),
            compression: target_compression,
            prefix_len: ctx.options.prefix_len,
            block_property_collectors: Vec::new(),
        };

        // Compute split points from target-level file boundaries.
        // L0 files overlap arbitrarily, so every sub-task would have to
        // include all L0 sources — redundant I/O with no benefit.
        // Sub-compaction is only useful for Ln→Ln+1 (level >= 1).
        //
        // Range deletions do NOT require serializing to a single sub-task:
        // Range tombstones below are collected
        // once from ALL input files and shared (by reference) with every
        // sub-task's `SubCompactionParams`; each sub-compaction then applies
        // exactly the subset overlapping its own key range (straddlers
        // included), so tombstone visibility stays complete for every key it
        // processes — the cross-sub-task sharing this data structure exists
        // for (INV-C2a) works correctly with `actual_subs > 1`.
        let max_subs = if task.level == 0 {
            1
        } else {
            ctx.options.max_subcompactions.max(1)
        };
        let split_points = compute_split_points(task, max_subs);
        let actual_subs = split_points.len() + 1;

        // Build sub-tasks
        let sub_tasks = build_sub_tasks(task, &split_points);

        // Collect ALL range tombstones once from ALL input files.
        // These must be shared across all sub-tasks because a single range
        // tombstone can span multiple sub-compaction key ranges.
        let all_input_files: Vec<TableFile> = task
            .input_files_level
            .iter()
            .chain(task.input_files_next.iter())
            .cloned()
            .collect();
        let range_fragments = collect_range_fragments(&all_input_files).ctx()?;

        let pending = PendingSsts::new(ctx.db_path, file_numbers);
        let sub_params = SubCompactionParams {
            target_level,
            is_bottommost,
            deeper_disjoint_levels: task.deeper_disjoint_levels.as_deref(),
            build_opts: &build_opts,
            oldest_snapshot_seq,
            pending: &pending,
            range_fragments: &range_fragments,
        };

        let run_range = |sub: &SubCompactionTask| {
            #[cfg(feature = "bench-metrics")]
            let _range = ctx
                .stats
                .filter(|_| task.level == 0)
                .map(|s| super::profile::ActiveRange::enter(&s.l0_profile));
            execute_sub_compaction_io(ctx, sub, &sub_params)
        };
        let sub_outputs = if actual_subs <= 1 {
            // Fast path: single sub-compaction (zero overhead)
            vec![run_range(&sub_tasks[0]).ctx()?]
        } else {
            // Parallel sub-compactions
            #[cfg(test)]
            PARALLEL_SUB_COMPACTIONS_TAKEN.fetch_add(1, Ordering::Relaxed);
            let thread_results: Vec<Result<SubCompactionOutput>> = scope(|s| {
                let mut handles = Vec::with_capacity(sub_tasks.len());
                let mut spawn_err: Option<Error> = None;
                for (idx, sub) in sub_tasks.iter().enumerate() {
                    #[cfg(test)]
                    if FAIL_SUB_COMPACTION_SPAWN_AT.with(|f| f.get()) == Some(idx) {
                        spawn_err = Some(
                            Error::from(std::io::Error::new(
                                std::io::ErrorKind::WouldBlock,
                                "injected sub-compaction thread spawn failure",
                            ))
                            .context("failed to spawn sub-compaction thread"),
                        );
                        break;
                    }
                    let _ = idx;
                    match thread::Builder::new().spawn_scoped(s, || {
                        let _build = ctx.builds.enter();
                        run_range(sub)
                    }) {
                        Ok(handle) => handles.push(handle),
                        Err(err) => {
                            spawn_err = Some(
                                Error::from(err).context("failed to spawn sub-compaction thread"),
                            );
                            break;
                        }
                    }
                }
                let mut results =
                    Vec::with_capacity(handles.len() + usize::from(spawn_err.is_some()));
                for h in handles {
                    results.push(match h.join() {
                        Ok(r) => r,
                        Err(_) => Err(Error::background("sub-compaction thread panicked")),
                    });
                }
                if let Some(err) = spawn_err {
                    results.push(Err(err));
                }
                results
            });
            thread_results.into_iter().collect::<Result<Vec<_>>>()?
        };

        // Merge sub-compaction outputs
        let mut edit = VersionEdit::new();
        let mut output_tombstones = OutputTombstones::new();
        let mut filter_mutated = false;
        for sub_out in sub_outputs {
            for file_entry in sub_out.new_files {
                edit.new_files.push(file_entry);
            }
            // File numbers come from a shared atomic counter, so per-sub maps
            // are disjoint and extend cannot collide.
            output_tombstones.extend(sub_out.output_tombstones);
            filter_mutated |= sub_out.filter_mutated;
        }

        #[cfg(feature = "bench-metrics")]
        if task.level == 0
            && let Some(stats) = ctx.stats
        {
            let mut p = stats.l0_profile.lock();
            p.rewrite_jobs += 1;
            p.multiple_target_jobs += u64::from(task.input_files_next.len() > 1);
            p.parallel_jobs += u64::from(actual_subs > 1);
            p.input_file_bytes += all_input_files
                .iter()
                .map(|f| f.meta.file_size)
                .sum::<u64>();
            p.output_file_bytes += edit.new_files.iter().map(|(_, m)| m.file_size).sum::<u64>();
        }

        // Record deletions (orchestrator responsibility)
        let input_files: Vec<(u32, FileMetaData)> = task
            .input_files_level
            .iter()
            .map(|f| (task.level as u32, f.meta.clone()))
            .chain(
                task.input_files_next
                    .iter()
                    .map(|f| (target_level as u32, f.meta.clone())),
            )
            .collect();
        let input_file_numbers: HashSet<u64> =
            input_files.iter().map(|(_, meta)| meta.number).collect();

        for tf in &task.input_files_level {
            edit.delete_file(task.level as u32, tf.meta.number);
        }
        for tf in &task.input_files_next {
            edit.delete_file(target_level as u32, tf.meta.number);
        }

        let dir_synced = sync_new_sst_parent_dir(ctx.dir_sync, ctx.manifest_poisoned)?;
        let tables = VersionSet::prepare_tables(
            ctx.db_path,
            ctx.table_cache,
            edit.new_files.iter().map(|(_, meta)| meta),
        )?;
        pending.keep();
        Ok(CompactionOutput {
            dir_synced,
            tables,
            edit,
            input_files,
            input_file_numbers,

            output_tombstones,
            filter_mutated,
        })
    }

    /// Install the result of a compaction: apply the VersionEdit, evict
    /// old files from cache, and delete old SST files from disk.
    /// Requires `&mut VersionSet` (hold the lock for this short phase only).
    pub fn install_compaction(
        mut output: CompactionOutput,
        versions: &mut VersionSet,
        table_cache: Option<&Arc<TableCache>>,
        block_cache: Option<&Arc<BlockCache>>,
        db_path: &Path,
        stats: Option<&Arc<DbStats>>,
        live_snapshots_present: bool,
    ) -> Result<PostCompactionCleanup> {
        // Files this compaction physically CREATED. A trivial move's "new"
        // file is the live input file itself (same number, relabeled to the
        // target level), so it must never be deleted or evicted by the
        // discard/error paths below — the current version still references
        // it at the source level, and deleting it would permanently destroy
        // data the MANIFEST points at. Normal outputs use freshly allocated
        // numbers that can never collide with an input's.
        let input_numbers: HashSet<u64> = output
            .input_files
            .iter()
            .map(|(_, meta)| meta.number)
            .collect();
        let created_files: Vec<(u32, FileMetaData)> = output
            .edit
            .new_files
            .iter()
            .filter(|(_, meta)| !input_numbers.contains(&meta.number))
            .cloned()
            .collect();

        // Guard against stale compaction results. If any input file has already
        // been removed, or if a concurrent install added an unexpected target-level
        // overlap while this compaction was doing I/O, this output is based on an
        // outdated version and must be discarded. A filter mutation captured
        // against an empty snapshot list must also be discarded if a snapshot
        // was registered before install: that snapshot has to observe the
        // pre-filter value.
        let snapshot_raced = output.filter_mutated && live_snapshots_present;
        if snapshot_raced {
            tracing::warn!(
                "discarding compaction output: filter changed values after a snapshot was registered"
            );
        }
        let discard = {
            let version = versions.current();
            let stale = output.input_files.iter().any(|(level, expected)| {
                let level = *level as usize;
                level >= version.num_levels
                    || !version
                        .level_files(level)
                        .iter()
                        .any(|tf| tf.meta == *expected)
            });
            snapshot_raced
                || stale
                || Self::outputs_overlap_unexpected_current_files(&output, &version)
        };
        if discard {
            cleanup_output_files(db_path, &created_files);
            evict_table_cache_files(table_cache, &created_files);
            return Ok(PostCompactionCleanup {
                files_to_delete: HashSet::new(),
                installed: false,
            });
        }

        output
            .edit
            .set_next_file_number(versions.next_file_number());
        if let Err(e) =
            versions.apply_with_tables(output.edit, output.dir_synced, Some(output.tables))
        {
            // Delete only the freshly-written SSTs (otherwise they would be
            // orphaned on disk) — `created_files` excludes a trivial move's
            // still-live input file.
            cleanup_output_files(db_path, &created_files);
            evict_table_cache_files(table_cache, &created_files);
            return Err(e).ctx();
        }

        // Evict from caches (fast, safe before sync)
        for num in &output.input_file_numbers {
            if let Some(cache) = table_cache {
                cache.evict(*num);
            }
            if let Some(bc) = block_cache {
                bc.invalidate_file(*num);
            }
        }

        if let Some(s) = stats {
            s.record_compaction_completed();
        }

        // Return the file numbers that need deletion AFTER manifest sync
        Ok(PostCompactionCleanup {
            files_to_delete: output.input_file_numbers,
            installed: true,
        })
    }

    /// Check whether any compaction output overlaps a target-level file that
    /// this edit neither deletes nor produced — evidence that a concurrent
    /// install raced the I/O phase. Installing such an output would break the
    /// L1+ disjointness invariant, so it must be discarded.
    ///
    /// Runs under the DB lock: output extents come from the tombstone ranges
    /// captured at build time (`output.output_tombstones`) — no output SST is
    /// opened or read here. Current-version files are checked through their
    /// long-lived readers, whose range tombstones are cached after first
    /// access (same pattern the pick phase uses).
    fn outputs_overlap_unexpected_current_files(
        output: &CompactionOutput,
        version: &Version,
    ) -> bool {
        let deleted: HashSet<(u32, u64)> = output.edit.deleted_files.iter().copied().collect();
        for (level, meta) in &output.edit.new_files {
            let level_idx = *level as usize;
            if level_idx == 0 || level_idx >= version.num_levels {
                continue;
            }

            let mut extents = Vec::new();
            if !meta.smallest_key.is_empty() && !meta.largest_key.is_empty() {
                extents.push(UserKeyRange::File {
                    smallest: user_key(&meta.smallest_key).to_vec(),
                    largest: user_key(&meta.largest_key).to_vec(),
                });
            }
            if let Some(tombstones) = output.output_tombstones.get(&meta.number) {
                extents.extend(
                    tombstones
                        .iter()
                        .map(|(begin, end)| UserKeyRange::Tombstone {
                            begin: begin.clone(),
                            end: end.clone(),
                        }),
                );
            }
            if extents.is_empty() {
                continue;
            }

            for current in version.level_files(level_idx) {
                if deleted.contains(&(*level, current.meta.number)) {
                    continue;
                }
                if extents
                    .iter()
                    .any(|extent| file_overlaps_extent(current, extent))
                {
                    tracing::warn!(
                        "discarding stale compaction output {:06}: overlaps current L{} file {:06}",
                        meta.number,
                        level_idx,
                        current.meta.number
                    );
                    return true;
                }
            }
        }
        false
    }

    /// Delete old SST files after manifest has been synced.
    pub fn run_post_compaction_cleanup(cleanup: &PostCompactionCleanup, db_path: &Path) {
        for num in &cleanup.files_to_delete {
            let old_path = db_path.join(format!("{:06}.sst", num));
            if let Err(e) = remove_file(&old_path) {
                tracing::warn!("failed to remove old SST {}: {}", old_path.display(), e);
            }
        }
    }

    /// Select a complete level for an in-place rewrite, using the same merge
    /// and install checks as organic compaction.
    pub(crate) fn full_level_task(version: &Version, level: usize) -> Option<CompactionTask> {
        let files = version.level_files(level);
        (!files.is_empty()).then(|| CompactionTask {
            level,
            target_level: level,
            input_files_level: files.to_vec(),
            input_files_next: Vec::new(),
            deeper_disjoint_levels: Some(
                version.files[(level + 1).min(version.num_levels)..].to_vec(),
            ),
        })
    }

    /// Pick a compaction based on a read-triggered hint when no organic L0 or
    /// byte-budget compaction is pending and the hinted level is over budget
    /// (or has a true metadata-only trivial move available when `allow_trivial_move`
    /// is true). Caller evaluates `allow_trivial_move` outside `DB::inner`.
    pub fn pick_compaction_for_hint(
        version: &Version,
        options: &DbOptions,
        hint: &CompactionHint,
        in_flight: &HashSet<u64>,
        allow_trivial_move: bool,
    ) -> Option<CompactionTask> {
        if Self::is_pending(version, options) {
            return None;
        }
        let level = hint.level;
        if level == 0 || level >= version.num_levels.saturating_sub(1) {
            return None;
        }
        let files = version.level_files(level);
        if files.len() <= 1 {
            return None;
        }
        let over_budget = Self::level_over_budget(version, options, level);
        if !over_budget && !allow_trivial_move {
            return None;
        }
        let task = Self::pick_level_compaction(version, level, in_flight)?;
        let is_trivial_move = allow_trivial_move
            && task.input_files_level.len() == 1
            && task.input_files_next.is_empty();
        if over_budget || is_trivial_move {
            Some(task)
        } else {
            None
        }
    }

    /// Maximum bytes for a given level.
    fn max_bytes_for_level(options: &DbOptions, level: usize) -> u64 {
        let mut result = options.max_bytes_for_level_base;
        if level <= 1 || result == 0 || options.max_bytes_for_level_multiplier == 1.0 {
            return result;
        }
        for _ in 1..level {
            #[cfg(test)]
            LEVEL_BUDGET_MULS.with(|n| n.set(n.get() + 1));
            result = (result as f64 * options.max_bytes_for_level_multiplier) as u64;
            if result == 0 || result == u64::MAX {
                break;
            }
        }
        result
    }

    /// Check whether a compaction range is effectively bottommost.
    ///
    /// `first_checked_level` is the source level for a moving compaction and the
    /// rewritten level for an in-place forced merge. Starting at the source is
    /// required because an unselected sibling range tombstone can extend beyond
    /// its file metadata and still cover a selected point entry.
    pub(crate) fn is_bottommost_level(
        version: &Version,
        first_checked_level: usize,
        num_levels: usize,
        all_inputs: &[TableFile],
    ) -> bool {
        let mut extents = Vec::new();
        for tf in all_inputs {
            add_file_extents(tf, &mut extents);
        }
        if extents.is_empty() {
            return true;
        }

        let input_numbers: HashSet<u64> = all_inputs.iter().map(|tf| tf.meta.number).collect();
        for level in first_checked_level..num_levels {
            for f in version.level_files(level) {
                if input_numbers.contains(&f.meta.number) {
                    continue;
                }
                if extents.iter().any(|extent| file_overlaps_extent(f, extent)) {
                    return false;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::FileNumbers;
    use crate::db::DB;
    use crate::options::DbOptions;

    #[test]
    fn compact_keeps_range_endpoints_shared_across_snapshot_stripes() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(
            DbOptions::default()
                .with_l0_compaction_trigger(usize::MAX)
                .with_block_cache_capacity(0)
                .with_pin_l0_first_block_and_warm_index(false),
            dir.path(),
        )
        .unwrap();
        db.put(b"m", b"initial").unwrap();
        let first = db.snapshot();
        let mut snapshots = Vec::new();
        for _ in 0..256 {
            db.delete_range(b"a", b"z").unwrap();
            snapshots.push(db.snapshot());
        }
        // This single 64 KiB boundary now belongs to two elementary
        // intervals containing all 256 historical deletion sequences.
        db.delete_range(&vec![b'k'; 64 * 1024], b"z").unwrap();
        db.flush().unwrap();
        db.put(b"z", b"keep").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("2"));
        let (result, allocation) = crate::test_alloc::measure(|| db.compact());
        result.unwrap();
        assert!(
            allocation.peak_live < 8 * 1024 * 1024,
            "compaction retained {} bytes while merging two packed intervals",
            allocation.peak_live
        );
        assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("0"));
        assert_eq!(
            first.get(b"m").unwrap().as_deref(),
            Some(b"initial".as_slice())
        );
        assert!(
            snapshots
                .iter()
                .all(|snap| snap.get(b"m").unwrap().is_none())
        );
        assert_eq!(db.get(b"z").unwrap().as_deref(), Some(b"keep".as_slice()));
    }

    #[test]
    fn l0_compaction_includes_l1_range_tombstone_extent() {
        // A range tombstone's end key is not in SST smallest/largest metadata.
        // After the tombstone sits alone at L1, an L0 point inside [begin, end)
        // must pull that L1 file in. Metadata-only overlap misses it, install
        // discards the output, and the same pick repeats.
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        db.delete_range(b"a", b"z").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("0"));
        assert_eq!(db.get_property("num-files-at-level1").as_deref(), Some("1"));

        db.put(b"m", b"v").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"m").unwrap().as_deref(), Some(b"v".as_slice()));
        assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("1"));

        db.compact().unwrap();
        assert_eq!(
            db.get(b"m").unwrap().as_deref(),
            Some(b"v".as_slice()),
            "newer point must survive the L1 range tombstone"
        );
        assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("0"));
    }

    #[test]
    fn test_discarded_trivial_move_preserves_live_input_file() {
        use std::collections::HashSet;

        use super::{CompactionOutput, LeveledCompaction, OutputTombstones};
        use crate::manifest::version_edit::{FileMetaData, VersionEdit};
        use crate::manifest::version_set::VersionSet;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        fn build_sst(
            dir: &std::path::Path,
            number: u64,
            first: &[u8],
            last: &[u8],
        ) -> FileMetaData {
            let path = dir.join(format!("{:06}.sst", number));
            let mut builder = TableBuilder::new(
                &path,
                TableBuildOptions {
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            for key in [first, last] {
                let ik = InternalKey::new(key, 1, ValueType::Value).into_bytes();
                builder.add(&ik, b"v").unwrap();
            }
            let result = builder.finish().unwrap();
            FileMetaData {
                number,
                file_size: result.file_size,
                smallest_key: result.smallest_key.unwrap(),
                largest_key: result.largest_key.unwrap(),
                has_range_deletions: false,
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let mut versions = VersionSet::create(dir.path(), 4).unwrap();

        // Live input file #1 at L1, range [e, f].
        let meta1 = build_sst(dir.path(), 1, b"e", b"f");
        let mut edit = VersionEdit::new();
        edit.add_file(1, meta1.clone());
        edit.set_next_file_number(versions.next_file_number());
        versions.log_and_apply(edit).unwrap();

        // Conflicting file #2 at L2 spanning [a, z] — simulates a concurrent
        // install (e.g. a force_merge_level hull output) landing while the
        // trivial move below was in its unlocked pick→install window.
        let meta2 = build_sst(dir.path(), 2, b"a", b"z");
        let mut edit = VersionEdit::new();
        edit.add_file(2, meta2.clone());
        edit.set_next_file_number(versions.next_file_number());
        versions.log_and_apply(edit).unwrap();

        // The trivial-move output exactly as execute_compaction_io builds it:
        // relabel file #1 from L1 to L2. `input_file_numbers` is empty
        // because no new physical file was written — the "new" file at L2 is
        // the same live 000001.sst.
        let mut edit = VersionEdit::new();
        edit.delete_file(1, meta1.number);
        edit.add_file(2, meta1.clone());
        let output = CompactionOutput {
            tables: vec![versions.current().level_files(1)[0].clone()],
            dir_synced: super::sync_new_sst_parent_dir(
                &versions.dir_sync_handle(),
                &versions.poison_flag(),
            )
            .unwrap(),
            edit,
            input_files: vec![(1, meta1.clone())],
            input_file_numbers: HashSet::new(),

            output_tombstones: OutputTombstones::new(),
            filter_mutated: false,
        };

        // Install must DISCARD the move (file #1's range now overlaps the
        // unexpected live L2 file #2, so installing would break L2
        // disjointness) — and the discard must NOT physically delete the
        // still-referenced input file.
        let cleanup = LeveledCompaction::install_compaction(
            output,
            &mut versions,
            None,
            None,
            dir.path(),
            None,
            false,
        )
        .unwrap();

        assert!(
            cleanup.files_to_delete.is_empty(),
            "a discarded install must not schedule any deletions"
        );
        assert!(
            dir.path().join("000001.sst").exists(),
            "discarding a trivial move must not delete the live input SST"
        );
        let version = versions.current();
        assert!(
            version
                .level_files(1)
                .iter()
                .any(|tf| tf.meta.number == meta1.number),
            "input file must remain at L1 after the discard"
        );
        assert!(
            version
                .level_files(2)
                .iter()
                .all(|tf| tf.meta.number != meta1.number),
            "discarded move must not appear at L2"
        );
    }

    #[test]
    fn test_level_picker_closes_source_range_tombstone_extent() {
        use std::collections::HashSet;

        use super::LeveledCompaction;
        use crate::manifest::version_edit::{FileMetaData, VersionEdit};
        use crate::manifest::version_set::VersionSet;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        fn build_sst(
            dir: &std::path::Path,
            number: u64,
            key: InternalKey,
            value: &[u8],
        ) -> FileMetaData {
            let path = dir.join(format!("{number:06}.sst"));
            let mut builder = TableBuilder::new(
                &path,
                TableBuildOptions {
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            builder.add(key.as_bytes(), value).unwrap();
            let result = builder.finish().unwrap();
            FileMetaData {
                number,
                file_size: result.file_size,
                smallest_key: result.smallest_key.unwrap(),
                largest_key: result.largest_key.unwrap(),
                has_range_deletions: result.has_range_deletions,
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let mut versions = VersionSet::create(dir.path(), 3).unwrap();
        let tombstone = build_sst(
            dir.path(),
            1,
            InternalKey::new(b"a", 5, ValueType::RangeDeletion),
            b"z",
        );
        let value = build_sst(
            dir.path(),
            2,
            InternalKey::new(b"m", 10, ValueType::Value),
            &[7u8; 8192],
        );
        assert!(
            value.file_size > tombstone.file_size,
            "the value file must be the initial largest-file pick"
        );

        let mut edit = VersionEdit::new();
        edit.add_file(1, tombstone.clone());
        edit.add_file(1, value.clone());
        versions.log_and_apply(edit).unwrap();

        let version = versions.current();
        let task = LeveledCompaction::pick_level_compaction(&version, 1, &HashSet::new()).unwrap();
        let picked: HashSet<u64> = task
            .input_files_level
            .iter()
            .map(|tf| tf.meta.number)
            .collect();
        assert_eq!(
            picked,
            HashSet::from([tombstone.number, value.number]),
            "the source-level tombstone covering m must move with the value file"
        );
    }

    #[test]
    fn test_range_picker_closes_source_range_tombstone_extent() {
        use std::collections::HashSet;

        use super::LeveledCompaction;
        use crate::manifest::version_edit::{FileMetaData, VersionEdit};
        use crate::manifest::version_set::VersionSet;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        fn build_sst(
            dir: &std::path::Path,
            number: u64,
            key: InternalKey,
            value: &[u8],
        ) -> FileMetaData {
            let path = dir.join(format!("{number:06}.sst"));
            let mut builder = TableBuilder::new(
                &path,
                TableBuildOptions {
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            builder.add(key.as_bytes(), value).unwrap();
            let result = builder.finish().unwrap();
            FileMetaData {
                number,
                file_size: result.file_size,
                smallest_key: result.smallest_key.unwrap(),
                largest_key: result.largest_key.unwrap(),
                has_range_deletions: result.has_range_deletions,
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let mut versions = VersionSet::create(dir.path(), 3).unwrap();
        // Tombstone's own metadata (smallest=largest="a") overlaps the
        // requested [a, b) range directly, but its real reach ("a".."z",
        // carried only in the range-tombstone block) covers `value`'s key
        // "m" — which sits far outside the requested range and has no
        // range deletions of its own, so the bounds-only filter alone would
        // never select it.
        let tombstone = build_sst(
            dir.path(),
            1,
            InternalKey::new(b"a", 5, ValueType::RangeDeletion),
            b"z",
        );
        let value = build_sst(
            dir.path(),
            2,
            InternalKey::new(b"m", 10, ValueType::Value),
            b"v",
        );

        let mut edit = VersionEdit::new();
        edit.add_file(0, tombstone.clone());
        edit.add_file(0, value.clone());
        versions.log_and_apply(edit).unwrap();

        let version = versions.current();
        let task = LeveledCompaction::pick_compaction_for_range(
            &version,
            Some(b"a"),
            Some(b"b"),
            &HashSet::new(),
        )
        .unwrap();
        assert_eq!(task.level, 0);
        let picked: HashSet<u64> = task
            .input_files_level
            .iter()
            .map(|tf| tf.meta.number)
            .collect();
        assert_eq!(
            picked,
            HashSet::from([tombstone.number, value.number]),
            "the covered value file must move with the range tombstone even though \
             its key falls outside the requested range"
        );
    }

    #[test]
    fn test_compaction_trigger() {
        let dir = tempfile::tempdir().unwrap();

        // Use small memtable to create multiple L0 files
        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            l0_compaction_trigger: 4,
            target_file_size_base: 1024 * 1024,
            ..Default::default()
        };
        let db = DB::open(opts.clone(), dir.path()).unwrap();

        // Write enough data to create multiple L0 SSTs
        for i in 0..200 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:040}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Check number of SST files
        let sst_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".sst")
            })
            .count();

        assert!(sst_count > 0, "should have SST files");

        // All data should still be readable
        for i in 0..200 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:040}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "failed at key {}",
                i
            );
        }
    }

    #[test]
    fn test_manual_compaction() {
        let dir = tempfile::tempdir().unwrap();

        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            l0_compaction_trigger: 100, // don't auto-compact
            ..Default::default()
        };
        let db = DB::open(opts.clone(), dir.path()).unwrap();

        // Write and flush multiple times to create L0 files
        for batch in 0..5 {
            for i in 0..20 {
                let key = format!("key_{:04}", batch * 20 + i);
                let val = format!("val_{}", batch * 20 + i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }

        // Trigger compaction
        db.compact().unwrap();

        // Verify all data
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "failed at key {} after compaction",
                i
            );
        }
    }

    #[test]
    fn test_compaction_removes_tombstones() {
        let dir = tempfile::tempdir().unwrap();

        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            l0_compaction_trigger: 100,
            num_levels: 2, // Only L0 and L1, so L1 is bottom
            ..Default::default()
        };
        let db = DB::open(opts, dir.path()).unwrap();

        // Write then delete
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            db.put(key.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();

        for i in 0..10 {
            let key = format!("key_{:04}", i);
            db.delete(key.as_bytes()).unwrap();
        }
        db.flush().unwrap();

        // Compact
        db.compact().unwrap();

        // Deleted keys should be gone
        for i in 0..10 {
            let key = format!("key_{:04}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), None);
        }

        // Remaining keys should exist
        for i in 10..20 {
            let key = format!("key_{:04}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(b"value".to_vec()));
        }
    }

    #[test]
    fn test_compaction_with_overwrites() {
        let dir = tempfile::tempdir().unwrap();

        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            l0_compaction_trigger: 100,
            ..Default::default()
        };
        let db = DB::open(opts, dir.path()).unwrap();

        // Write v1
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            db.put(key.as_bytes(), b"v1").unwrap();
        }
        db.flush().unwrap();

        // Overwrite with v2
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            db.put(key.as_bytes(), b"v2").unwrap();
        }
        db.flush().unwrap();

        db.compact().unwrap();

        // Should see v2
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(b"v2".to_vec()),
                "key {} should have value v2 after compaction",
                i
            );
        }
    }

    #[test]
    fn test_sub_compaction_correctness() {
        let dir = tempfile::tempdir().unwrap();

        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            l0_compaction_trigger: 100,
            target_file_size_base: 512, // Small files to create many L1 files
            max_subcompactions: 4,
            ..Default::default()
        };
        let db = DB::open(opts, dir.path()).unwrap();

        // Write enough data to create multiple L0 files
        for batch in 0..8 {
            for i in 0..50 {
                let key = format!("key_{:06}", batch * 50 + i);
                let val = format!("value_{:040}", batch * 50 + i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }

        // Compact with sub-compaction
        db.compact().unwrap();

        // Verify all data is intact
        for i in 0..400 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:040}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "key {} missing after sub-compaction",
                i
            );
        }
    }

    #[test]
    fn test_sub_compaction_with_deletions() {
        let dir = tempfile::tempdir().unwrap();

        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            l0_compaction_trigger: 100,
            target_file_size_base: 512,
            max_subcompactions: 4,
            num_levels: 2, // bottommost for tombstone GC
            ..Default::default()
        };
        let db = DB::open(opts, dir.path()).unwrap();

        // Write keys
        for i in 0..100 {
            let key = format!("key_{:06}", i);
            db.put(key.as_bytes(), b"v1").unwrap();
        }
        db.flush().unwrap();

        // Delete even keys
        for i in (0..100).step_by(2) {
            let key = format!("key_{:06}", i);
            db.delete(key.as_bytes()).unwrap();
        }
        db.flush().unwrap();

        // Overwrite some odd keys
        for i in (1..100).step_by(2) {
            let key = format!("key_{:06}", i);
            db.put(key.as_bytes(), b"v2").unwrap();
        }
        db.flush().unwrap();

        db.compact().unwrap();

        // Even keys should be gone
        for i in (0..100).step_by(2) {
            let key = format!("key_{:06}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                None,
                "key {} should be deleted",
                i
            );
        }
        // Odd keys should have v2
        for i in (1..100).step_by(2) {
            let key = format!("key_{:06}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(b"v2".to_vec()),
                "key {} should be v2",
                i
            );
        }
    }

    #[test]
    fn test_sub_compaction_with_range_tombstones() {
        let dir = tempfile::tempdir().unwrap();

        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            l0_compaction_trigger: 100,
            target_file_size_base: 512,
            max_subcompactions: 4,
            num_levels: 2,
            ..Default::default()
        };
        let db = DB::open(opts, dir.path()).unwrap();

        // Write keys spanning a wide range
        for i in 0..200 {
            let key = format!("key_{:06}", i);
            db.put(key.as_bytes(), b"val").unwrap();
        }
        db.flush().unwrap();

        // Delete a range that likely spans sub-compaction boundaries
        db.delete_range(b"key_000050", b"key_000150").unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        // Keys outside range should exist
        for i in 0..50 {
            let key = format!("key_{:06}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(b"val".to_vec()),
                "key {} should survive",
                i
            );
        }
        for i in 150..200 {
            let key = format!("key_{:06}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(b"val".to_vec()),
                "key {} should survive",
                i
            );
        }
        // Keys in range should be deleted
        for i in 50..150 {
            let key = format!("key_{:06}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                None,
                "key {} should be deleted",
                i
            );
        }
    }

    /// A range tombstone whose sequence is `MAX_SEQUENCE_NUMBER` and whose
    /// begin key is a sub-compaction split must be written by the sub-job
    /// that owns that user key. Seeking that job at `(user, MAX, Value)`
    /// skips the tombstone, and the previous job already stopped before it.
    #[test]
    fn max_sequence_range_tombstone_at_split_is_written() {
        use std::sync::Arc;

        use crate::manifest::version::TableFile;
        use crate::manifest::version_edit::FileMetaData;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::sst::table_reader::TableReader;
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let opts = TableBuildOptions::default();

        let write = |number: u64, key: &[u8], seq: u64, vt: ValueType, value: &[u8]| {
            let path = dir.path().join(format!("{number:06}.sst"));
            let mut builder = TableBuilder::new(&path, opts.clone()).unwrap();
            let ikey = InternalKey::new(key, seq, vt);
            builder.add(ikey.as_bytes(), value).unwrap();
            let result = builder.finish().unwrap();
            let reader = Arc::new(TableReader::open(&path).unwrap());
            TableFile {
                meta: FileMetaData {
                    number,
                    file_size: result.file_size,
                    smallest_key: result.smallest_key.unwrap(),
                    largest_key: result.largest_key.unwrap(),
                    has_range_deletions: result.has_range_deletions,
                },
                reader,
            }
        };

        // L1 tombstone [m, z) at the maximum sequence. Two L2 files make the
        // split point `m`, so the tombstone belongs only to the second job.
        let l1 = write(
            1,
            b"m",
            crate::types::MAX_SEQUENCE_NUMBER,
            ValueType::RangeDeletion,
            b"z",
        );
        let l2_left = write(2, b"m", 1, ValueType::Value, b"left");
        let l2_right = write(3, b"z", 1, ValueType::Value, b"right");
        let task = super::CompactionTask {
            level: 1,
            target_level: 2,
            input_files_level: vec![l1],
            input_files_next: vec![l2_left, l2_right],
            deeper_disjoint_levels: None,
        };
        let splits = super::compute_split_points(&task, 2);
        assert_eq!(splits, vec![b"m".to_vec()]);

        let options = DbOptions {
            max_subcompactions: 2,
            ..DbOptions::default()
        };
        let dir_sync = super::Mutex::new(Some(super::File::open(dir.path()).unwrap()));
        let ctx = super::CompactionContext {
            dir_sync: &dir_sync,
            manifest_poisoned: &super::AtomicBool::new(false),
            db_path: dir.path(),
            table_cache: None,
            options: &options,
            builds: &super::SstBuilds::default(),
            rate_limiter: None,
            stats: None,
            active_snapshots: &[],
        };
        let output = super::LeveledCompaction::execute_compaction_io(
            &ctx,
            &task,
            &FileNumbers::new(10),
            false,
        )
        .unwrap();
        let kept = output
            .output_tombstones
            .values()
            .flatten()
            .any(|(begin, end)| begin.as_slice() == b"m" && end.as_slice() == b"z");
        assert!(
            kept,
            "range tombstone at the split key and MAX_SEQUENCE_NUMBER must be written"
        );
    }

    #[test]
    fn test_compact_range_keeps_range_tombstone_until_covered_files_included() {
        let dir = tempfile::tempdir().unwrap();

        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 1024 * 1024,
            l0_compaction_trigger: 100,
            target_file_size_base: 512,
            num_levels: 2,
            ..Default::default()
        };
        let db = DB::open(opts, dir.path()).unwrap();

        let value = vec![b'v'; 96];
        for i in 0..200 {
            let key = format!("key_{:03}", i);
            db.put(key.as_bytes(), &value).unwrap();
        }
        db.flush().unwrap();
        db.compact().unwrap();

        db.delete_range(b"key_000", b"key_200").unwrap();
        db.flush().unwrap();

        db.compact_range(Some(b"key_000"), Some(b"key_001"))
            .unwrap();

        for i in [0, 1, 50, 150, 199] {
            let key = format!("key_{:03}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                None,
                "range-compacted tombstone must continue deleting {key}"
            );
        }
    }

    #[test]
    fn test_sub_compaction_with_snapshots() {
        let dir = tempfile::tempdir().unwrap();

        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            l0_compaction_trigger: 100,
            target_file_size_base: 512,
            max_subcompactions: 4,
            ..Default::default()
        };
        let db = DB::open(opts, dir.path()).unwrap();

        // Write v1
        for i in 0..100 {
            let key = format!("key_{:06}", i);
            db.put(key.as_bytes(), b"v1").unwrap();
        }
        db.flush().unwrap();

        // Take snapshot
        let snap = db.snapshot();
        let snap_opts = snap.read_options();

        // Write v2
        for i in 0..100 {
            let key = format!("key_{:06}", i);
            db.put(key.as_bytes(), b"v2").unwrap();
        }
        db.flush().unwrap();

        // Compact (snapshot should protect v1 versions)
        db.compact().unwrap();

        // Current reads see v2
        for i in 0..100 {
            let key = format!("key_{:06}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(b"v2".to_vec()),
                "current key {} should be v2",
                i
            );
        }

        // Snapshot reads see v1
        for i in 0..100 {
            let key = format!("key_{:06}", i);
            assert_eq!(
                db.get_with_options(&snap_opts, key.as_bytes()).unwrap(),
                Some(b"v1".to_vec()),
                "snapshot key {} should be v1",
                i
            );
        }

        drop(snap);
    }

    #[test]
    fn test_sub_compaction_end_to_end() {
        // Integration test: write data, compact with max_subcompactions=4,
        // then compact again to exercise multi-level sub-compaction.
        let dir = tempfile::tempdir().unwrap();

        let opts = DbOptions {
            create_if_missing: true,
            write_buffer_size: 512,
            l0_compaction_trigger: 100,
            target_file_size_base: 512,
            max_subcompactions: 4,
            ..Default::default()
        };
        let db = DB::open(opts, dir.path()).unwrap();

        // Write enough data across many flushes to create multiple L1 files
        for batch in 0..10 {
            for i in 0..50 {
                let key = format!("key_{:06}", batch * 50 + i);
                let val = format!("value_{:040}", batch * 50 + i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }

        // First compaction: L0 → L1 (creates multiple small L1 files)
        db.compact().unwrap();

        // Write more data and flush again to create new L0 files
        for batch in 0..5 {
            for i in 0..50 {
                let key = format!("key_{:06}", batch * 50 + i);
                let val = format!("updated_{:040}", batch * 50 + i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }

        // Second compaction: should exercise sub-compaction on L1→L2
        db.compact().unwrap();

        // Verify all data: first 250 keys have "updated_" values
        for i in 0..250 {
            let key = format!("key_{:06}", i);
            let val = format!("updated_{:040}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "key {} should have updated value",
                i
            );
        }
        // Keys 250..500 have original values
        for i in 250..500 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:040}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "key {} should have original value",
                i
            );
        }
    }

    #[test]
    fn test_execute_compaction_io_actually_parallelizes_with_max_subcompactions() {
        // Regression test: db.compact()/db.compact_range(None, None) always
        // route through force_compact_all -> force_merge_level, which never
        // reads max_subcompactions and never takes execute_compaction_io's
        // `thread::scope` parallel branch. Prior "sub_compaction"-named
        // tests in this file only exercised that always-serial path despite
        // setting max_subcompactions > 1, so the real parallel branch had
        // zero test coverage anywhere in the repository. This test calls
        // `execute_compaction_io` directly (mirroring
        // `test_discarded_trivial_move_preserves_live_input_file`'s
        // hand-built-VersionSet pattern) with enough target-level files to
        // force `actual_subs > 1`, and asserts — via the test-only
        // `PARALLEL_SUB_COMPACTIONS_TAKEN` counter — that the parallel path
        // was genuinely taken, not just that the (already well-tested)
        // serial merge logic produced correct output.
        use std::sync::Arc;
        use std::sync::atomic::Ordering as AtomicOrdering;

        use super::{
            CompactionContext, CompactionTask, LeveledCompaction, PARALLEL_SUB_COMPACTIONS_TAKEN,
        };
        use crate::manifest::version::TableFile;
        use crate::manifest::version_edit::FileMetaData;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, InternalKeyRef, ValueType};

        fn build_sst(dir: &std::path::Path, number: u64, keys: &[usize]) -> TableFile {
            let path = dir.join(format!("{:06}.sst", number));
            let mut builder = TableBuilder::new(
                &path,
                TableBuildOptions {
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut smallest = Vec::new();
            let mut largest = Vec::new();
            for (idx, &i) in keys.iter().enumerate() {
                let uk = format!("key_{:06}", i);
                let ik = InternalKey::new(uk.as_bytes(), 10, ValueType::Value);
                let val = format!("val_{}", i);
                if idx == 0 {
                    smallest = ik.as_bytes().to_vec();
                }
                largest = ik.as_bytes().to_vec();
                builder.add(ik.as_bytes(), val.as_bytes()).unwrap();
            }
            let result = builder.finish().unwrap();
            TableFile {
                meta: FileMetaData {
                    number,
                    file_size: result.file_size,
                    smallest_key: result.smallest_key.unwrap_or(smallest),
                    largest_key: result.largest_key.unwrap_or(largest),
                    has_range_deletions: false,
                },
                reader: Arc::new(crate::sst::table_reader::TableReader::open(&path).unwrap()),
            }
        }

        let dir = tempfile::tempdir().unwrap();

        // Source level (L1): one file spanning the whole key range 0..300.
        let source_keys: Vec<usize> = (0..300).collect();
        let source_file = build_sst(dir.path(), 1, &source_keys);

        // Target level (L2): 3 files with disjoint, evenly-spaced key
        // ranges, so `compute_split_points` has 2 real boundaries to work
        // with (`next_files.len() > 1`), forcing `actual_subs > 1` when
        // `max_subcompactions` is at least 3.
        let target_files = vec![
            build_sst(dir.path(), 2, &(0..100).collect::<Vec<_>>()),
            build_sst(dir.path(), 3, &(100..200).collect::<Vec<_>>()),
            build_sst(dir.path(), 4, &(200..300).collect::<Vec<_>>()),
        ];

        let task = CompactionTask {
            level: 1,
            target_level: 2,
            input_files_level: vec![source_file],
            input_files_next: target_files,
            deeper_disjoint_levels: None,
        };

        let options = DbOptions {
            max_subcompactions: usize::MAX,
            ..Default::default()
        };
        let dir_sync = super::Mutex::new(Some(super::File::open(dir.path()).unwrap()));
        let ctx = CompactionContext {
            dir_sync: &dir_sync,
            manifest_poisoned: &super::AtomicBool::new(false),
            db_path: dir.path(),
            table_cache: None,
            options: &options,
            builds: &super::SstBuilds::default(),
            rate_limiter: None,
            stats: None,
            active_snapshots: &[],
        };

        let before = PARALLEL_SUB_COMPACTIONS_TAKEN.load(AtomicOrdering::Relaxed);
        let output =
            LeveledCompaction::execute_compaction_io(&ctx, &task, &FileNumbers::new(100), true)
                .unwrap();
        let after = PARALLEL_SUB_COMPACTIONS_TAKEN.load(AtomicOrdering::Relaxed);

        assert!(
            after > before,
            "execute_compaction_io should have taken the parallel (actual_subs > 1) \
             path given 3 disjoint target-level files and \
             max_subcompactions=usize::MAX, but the counter did not increment — the \
             parallel branch has zero test coverage otherwise"
        );

        // Sanity: the merge still produced a correct, complete output
        // (multiple output files whose union covers the whole input range).
        let mut all_keys: Vec<u32> = Vec::new();
        for (_, meta) in &output.edit.new_files {
            let path = dir.path().join(format!("{:06}.sst", meta.number));
            let reader = crate::sst::table_reader::TableReader::open(&path).unwrap();
            for (k, _) in reader.iter().unwrap() {
                let ik = InternalKeyRef::new(&k);
                let uk = std::str::from_utf8(ik.user_key()).unwrap();
                let n: u32 = uk.trim_start_matches("key_").parse().unwrap();
                all_keys.push(n);
            }
        }
        all_keys.sort_unstable();
        let expected: Vec<u32> = (0..300).collect();
        assert_eq!(
            all_keys, expected,
            "parallel sub-compaction output must cover every input key exactly once"
        );
    }

    #[test]
    fn test_execute_compaction_io_parallelizes_with_range_deletions() {
        // Regression test: range deletions must not force execute_compaction_io
        // back to the serial path. A tombstone spanning a sub-compaction split
        // boundary must be visible to every sub-task so bottommost GC matches
        // the serial result exactly.
        use std::sync::Arc;
        use std::sync::atomic::Ordering as AtomicOrdering;

        use super::{
            CompactionContext, CompactionOutput, CompactionTask, LeveledCompaction,
            PARALLEL_SUB_COMPACTIONS_TAKEN,
        };
        use crate::manifest::version::TableFile;
        use crate::manifest::version_edit::FileMetaData;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, InternalKeyRef, ValueType, compare_internal_key};

        /// (user_key, sequence, value_type, value) for one decoded point entry.
        type PointEntry = (String, u64, ValueType, String);
        /// (begin_key, end_key, sequence) for one decoded range tombstone.
        type TombstoneEntry = (Vec<u8>, Vec<u8>, u64);

        fn key(i: usize) -> String {
            format!("key_{:06}", i)
        }

        fn value(prefix: &str, i: usize) -> String {
            format!("{}_{:06}", prefix, i)
        }

        fn build_sst(
            dir: &std::path::Path,
            number: u64,
            mut entries: Vec<(String, u64, ValueType, Vec<u8>)>,
        ) -> TableFile {
            entries.sort_by(|(ak, aseq, av, _), (bk, bseq, bv, _)| {
                compare_internal_key(
                    InternalKey::new(ak.as_bytes(), *aseq, *av).as_bytes(),
                    InternalKey::new(bk.as_bytes(), *bseq, *bv).as_bytes(),
                )
            });

            let path = dir.join(format!("{:06}.sst", number));
            let mut builder = TableBuilder::new(
                &path,
                TableBuildOptions {
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            for (user_key, seq, value_type, value) in entries {
                let ikey = InternalKey::new(user_key.as_bytes(), seq, value_type);
                builder.add(ikey.as_bytes(), &value).unwrap();
            }
            let result = builder.finish().unwrap();
            TableFile {
                meta: FileMetaData {
                    number,
                    file_size: result.file_size,
                    smallest_key: result.smallest_key.unwrap_or_default(),
                    largest_key: result.largest_key.unwrap_or_default(),
                    has_range_deletions: result.has_range_deletions,
                },
                reader: Arc::new(crate::sst::table_reader::TableReader::open(&path).unwrap()),
            }
        }

        fn build_inputs(dir: &std::path::Path) -> CompactionTask {
            let mut source_entries = Vec::new();
            for i in 0..300 {
                source_entries.push((
                    key(i),
                    10,
                    ValueType::Value,
                    value("source", i).into_bytes(),
                ));
            }
            for i in [130, 180] {
                source_entries.push((key(i), 60, ValueType::Value, value("fresh", i).into_bytes()));
            }
            source_entries.push((
                key(120),
                50,
                ValueType::RangeDeletion,
                key(220).into_bytes(),
            ));
            let source_file = build_sst(dir, 1, source_entries);

            let mut target_files = Vec::new();
            for (number, range) in [(2, 0..100), (3, 100..200), (4, 200..300)] {
                let entries = range
                    .map(|i| (key(i), 5, ValueType::Value, value("target", i).into_bytes()))
                    .collect();
                target_files.push(build_sst(dir, number, entries));
            }

            CompactionTask {
                level: 1,
                target_level: 2,
                input_files_level: vec![source_file],
                input_files_next: target_files,
                deeper_disjoint_levels: None,
            }
        }

        fn collect_output(
            dir: &std::path::Path,
            output: &CompactionOutput,
        ) -> (Vec<PointEntry>, Vec<TombstoneEntry>) {
            let mut points = Vec::new();
            let mut tombstones = Vec::new();
            for (_, meta) in &output.edit.new_files {
                let path = dir.join(format!("{:06}.sst", meta.number));
                let reader = crate::sst::table_reader::TableReader::open(&path).unwrap();
                for (ikey, value) in reader.iter().unwrap() {
                    let ikr = InternalKeyRef::new(&ikey);
                    points.push((
                        std::str::from_utf8(ikr.user_key()).unwrap().to_string(),
                        ikr.sequence(),
                        ikr.value_type(),
                        String::from_utf8(value).unwrap(),
                    ));
                }
                tombstones.extend(reader.get_range_tombstones().unwrap());
            }
            points.sort_by(|a, b| (&a.0, a.1, a.2 as u8, &a.3).cmp(&(&b.0, b.1, b.2 as u8, &b.3)));
            tombstones.sort();
            (points, tombstones)
        }

        fn run_compaction(
            max_subcompactions: usize,
            is_bottommost: bool,
            active_snapshots: &[u64],
            file_number_start: u64,
        ) -> (Vec<PointEntry>, Vec<TombstoneEntry>, usize) {
            let dir = tempfile::tempdir().unwrap();
            let task = build_inputs(dir.path());
            let options = DbOptions {
                max_subcompactions,
                ..Default::default()
            };
            let dir_sync = super::Mutex::new(Some(super::File::open(dir.path()).unwrap()));
            let ctx = CompactionContext {
                dir_sync: &dir_sync,
                manifest_poisoned: &super::AtomicBool::new(false),
                db_path: dir.path(),
                table_cache: None,
                options: &options,
                builds: &super::SstBuilds::default(),
                rate_limiter: None,
                stats: None,
                active_snapshots,
            };

            let before = PARALLEL_SUB_COMPACTIONS_TAKEN.load(AtomicOrdering::Relaxed);
            let output = LeveledCompaction::execute_compaction_io(
                &ctx,
                &task,
                &FileNumbers::new(file_number_start),
                is_bottommost,
            )
            .unwrap();
            let after = PARALLEL_SUB_COMPACTIONS_TAKEN.load(AtomicOrdering::Relaxed);
            let (points, tombstones) = collect_output(dir.path(), &output);
            (points, tombstones, after - before)
        }

        let (parallel_points, parallel_tombstones, parallel_count) =
            run_compaction(4, true, &[], 100);
        assert!(
            parallel_count >= 1,
            "range deletions must still exercise the parallel sub-compaction path"
        );
        assert!(
            parallel_tombstones.is_empty(),
            "bottommost compaction without snapshots should drop obsolete range tombstones"
        );

        let expected_points: Vec<_> = (0..300)
            .filter_map(|i| {
                if (120..220).contains(&i) && i != 130 && i != 180 {
                    None
                } else {
                    let prefix = if i == 130 || i == 180 {
                        "fresh"
                    } else {
                        "source"
                    };
                    Some((key(i), 0, ValueType::Value, value(prefix, i)))
                }
            })
            .collect();
        assert_eq!(
            parallel_points, expected_points,
            "parallel range-deletion compaction kept the wrong logical key set"
        );
        assert!(
            !parallel_points.iter().any(|(k, _, _, _)| {
                let n: usize = k.trim_start_matches("key_").parse().unwrap();
                (120..220).contains(&n) && n != 130 && n != 180
            }),
            "covered keys below the tombstone sequence must be absent"
        );

        let (serial_points, serial_tombstones, _serial_count) = run_compaction(1, true, &[], 200);
        assert_eq!(
            parallel_points, serial_points,
            "parallel and serial bottommost GC must produce identical logical output"
        );
        assert_eq!(parallel_tombstones, serial_tombstones);

        let (snapshot_points, snapshot_tombstones, snapshot_parallel_count) =
            run_compaction(4, false, &[25], 300);
        assert!(
            snapshot_parallel_count >= 1,
            "non-bottommost snapshot case should still take the parallel path"
        );
        let fragments: Vec<_> = snapshot_tombstones
            .iter()
            .filter(|(_, _, seq)| *seq == 50)
            .collect();
        assert!(fragments.len() > 1, "straddling tombstone must be clipped");
        assert_eq!(fragments[0].0, key(120).as_bytes());
        assert_eq!(fragments.last().unwrap().1, key(220).as_bytes());
        for pair in fragments.windows(2) {
            assert_eq!(
                pair[0].1, pair[1].0,
                "outputs must tile the original interval"
            );
        }
        assert!(
            snapshot_points.iter().any(|(k, seq, _, v)| {
                k == &key(150) && *seq == 10 && v == &value("source", 150)
            }),
            "covered keys must be retained when a snapshot below the tombstone needs them"
        );
    }

    /// Regression: a range tombstone can pull an earlier-key target file in a
    /// later closure pass. Returning targets in discovery order made
    /// sub-compaction split points non-monotonic, so two sub-jobs emitted the
    /// same keys and the overlapping install fail-stopped the DB.
    #[test]
    fn target_inputs_keep_level_order_for_split_points() {
        use std::sync::Arc;

        use super::{CompactionTask, compute_split_points, overlapping_files_for_inputs};
        use crate::manifest::version::TableFile;
        use crate::manifest::version_edit::FileMetaData;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::sst::table_reader::TableReader;
        use crate::types::{InternalKey, ValueType};

        fn build(
            dir: &std::path::Path,
            number: u64,
            entries: &[(InternalKey, &[u8])],
        ) -> TableFile {
            let path = dir.join(format!("{number:06}.sst"));
            let mut builder = TableBuilder::new(
                &path,
                TableBuildOptions {
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            for (key, value) in entries {
                builder.add(key.as_bytes(), value).unwrap();
            }
            let result = builder.finish().unwrap();
            TableFile {
                meta: FileMetaData {
                    number,
                    file_size: result.file_size,
                    smallest_key: result.smallest_key.unwrap(),
                    largest_key: result.largest_key.unwrap(),
                    has_range_deletions: result.has_range_deletions,
                },
                reader: Arc::new(TableReader::open(&path).unwrap()),
            }
        }
        let ik = |k: &[u8], seq, vt| InternalKey::new(k, seq, vt);

        let dir = tempfile::tempdir().unwrap();
        // Target level, sorted and disjoint. g1's tombstone [a, f) reaches g3.
        let g1 = build(
            dir.path(),
            1,
            &[
                (ik(b"a", 10, ValueType::Value), b"v"),
                (ik(b"a", 5, ValueType::RangeDeletion), b"f"),
                (ik(b"b", 11, ValueType::Value), b"v"),
            ],
        );
        let g2 = build(
            dir.path(),
            2,
            &[
                (ik(b"c", 12, ValueType::Value), b"v"),
                (ik(b"d", 13, ValueType::Value), b"v"),
            ],
        );
        let g3 = build(
            dir.path(),
            3,
            &[
                (ik(b"e", 14, ValueType::Value), b"v"),
                (ik(b"k", 15, ValueType::Value), b"v"),
            ],
        );
        // Source input overlapping only g3 by metadata.
        let f = build(
            dir.path(),
            4,
            &[
                (ik(b"h", 20, ValueType::Value), b"v"),
                (ik(b"j", 21, ValueType::Value), b"v"),
            ],
        );

        let targets = overlapping_files_for_inputs(&[g1, g2, g3], std::slice::from_ref(&f));
        let numbers: Vec<u64> = targets.iter().map(|tf| tf.meta.number).collect();
        assert_eq!(numbers, vec![1, 2, 3], "targets must stay in level order");

        let task = CompactionTask {
            level: 1,
            target_level: 2,
            input_files_level: vec![f],
            input_files_next: targets,
            deeper_disjoint_levels: None,
        };
        let splits = compute_split_points(&task, 3);
        assert!(
            splits.windows(2).all(|w| w[0] < w[1]),
            "split points must be strictly increasing: {splits:?}"
        );
    }

    /// Regression: the forced merge added each tombstone with `add` + `reset`,
    /// and every reset cleared the active set, so the next check re-activated
    /// every earlier tombstone: quadratic work while holding the DB mutex.
    #[test]
    fn forced_merge_activates_each_tombstone_once() {
        use super::{CompactionContext, LeveledCompaction};
        use crate::iterator::range_del::TRACKER_ACTIVATIONS;
        use crate::manifest::version_edit::{FileMetaData, VersionEdit};
        use crate::manifest::version_set::VersionSet;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        const PER_FILE: usize = 150;
        let options = crate::options::DbOptions::default();
        let dir = tempfile::tempdir().unwrap();
        let mut versions = VersionSet::create(dir.path(), options.num_levels).unwrap();
        let mut edit = VersionEdit::new();
        for file in 0..2u64 {
            let number = versions.file_numbers.allocate().unwrap();
            let path = dir.path().join(format!("{number:06}.sst"));
            let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
            for i in 0..PER_FILE {
                let n = file as usize * PER_FILE + i;
                let seq = 1 + n as u64;
                let point = InternalKey::new(format!("k{n:04}a").as_bytes(), seq, ValueType::Value);
                builder.add(point.as_bytes(), b"v").unwrap();
                let begin = format!("k{n:04}b");
                let tombstone = InternalKey::new(begin.as_bytes(), seq, ValueType::RangeDeletion);
                builder
                    .add(tombstone.as_bytes(), format!("k{n:04}c").as_bytes())
                    .unwrap();
            }
            let result = builder.finish().unwrap();
            edit.add_file(
                1,
                FileMetaData {
                    number,
                    file_size: result.file_size,
                    smallest_key: result.smallest_key.unwrap(),
                    largest_key: result.largest_key.unwrap(),
                    has_range_deletions: result.has_range_deletions,
                },
            );
        }
        edit.set_next_file_number(versions.next_file_number());
        versions.log_and_apply(edit).unwrap();

        let dir_sync = versions.dir_sync_handle();
        let ctx = CompactionContext {
            dir_sync: &dir_sync,
            manifest_poisoned: &super::AtomicBool::new(false),
            db_path: dir.path(),
            table_cache: None,
            options: &options,
            builds: &super::SstBuilds::default(),
            rate_limiter: None,
            stats: None,
            active_snapshots: &[],
        };
        TRACKER_ACTIVATIONS.with(|n| n.set(0));
        let task = LeveledCompaction::full_level_task(&versions.current(), 1).unwrap();
        let output =
            LeveledCompaction::execute_compaction_io(&ctx, &task, &versions.file_numbers, true)
                .unwrap();
        LeveledCompaction::install_compaction(
            output,
            &mut versions,
            None,
            None,
            dir.path(),
            None,
            false,
        )
        .unwrap();
        let activations = TRACKER_ACTIVATIONS.with(|n| n.get());
        assert!(
            activations <= 2 * 2 * PER_FILE,
            "{activations} activations for {} tombstones",
            2 * PER_FILE
        );
    }

    #[test]
    fn empty_and_deep_levels_keep_budget_checks_linear() {
        use super::{HashSet, LeveledCompaction};
        use crate::manifest::version::Version;
        let options = DbOptions {
            num_levels: 500,
            ..Default::default()
        };
        let version = Version::new(options.num_levels);

        super::LEVEL_BUDGET_MULS.with(|n| n.set(0));
        assert!(!LeveledCompaction::is_pending(&version, &options));
        assert!(LeveledCompaction::pick_compaction(&version, &options, &HashSet::new()).is_none());
        let empty_muls = super::LEVEL_BUDGET_MULS.with(|n| n.get());
        assert_eq!(
            empty_muls, 0,
            "empty levels must not run O(level) budget multiplications"
        );

        super::LEVEL_BUDGET_MULS.with(|n| n.set(0));
        let _ = LeveledCompaction::max_bytes_for_level(&options, 499);
        let deep_muls = super::LEVEL_BUDGET_MULS.with(|n| n.get());
        assert!(
            deep_muls <= 32,
            "saturated level budget should stop multiplying once u64::MAX is reached, got {deep_muls}"
        );

        let unit_opts = DbOptions {
            max_bytes_for_level_multiplier: 1.0,
            ..options
        };
        super::LEVEL_BUDGET_MULS.with(|n| n.set(0));
        assert_eq!(
            LeveledCompaction::max_bytes_for_level(&unit_opts, 499),
            unit_opts.max_bytes_for_level_base
        );
        assert_eq!(super::LEVEL_BUDGET_MULS.with(|n| n.get()), 0);
    }

    #[test]
    fn sub_compaction_preserves_typed_corruption_and_io_errors() {
        use crate::{DB, ErrorKind};
        use std::fs;

        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions::default()
            .with_l0_compaction_trigger(usize::MAX)
            .with_pin_l0_first_block_and_warm_index(false);
        let db = DB::open(opts, dir.path()).unwrap();
        db.put(b"k1", b"value_1").unwrap();
        db.flush().unwrap();
        db.put(b"k2", b"value_2").unwrap();
        db.flush().unwrap();

        let sst_path = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| p.extension().is_some_and(|ext| ext == "sst"))
            .unwrap();
        let mut bytes = fs::read(&sst_path).unwrap();
        bytes[0] ^= 0xff;
        fs::write(&sst_path, &bytes).unwrap();

        let err = db.compact().unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::Corruption,
            "expected Corruption, got {err:?}"
        );
        // Engine remains usable for reads on uncorrupted memtable keys since
        // synchronous compact() did not fail-stop background state.
        db.put(b"live", b"ok").unwrap();
        assert_eq!(db.get(b"live").unwrap().as_deref(), Some(b"ok".as_slice()));
    }

    #[test]
    fn compact_rejects_unordered_point_keys_with_corruption() {
        use crate::sst::block_builder::BlockBuilder;
        use crate::sst::format::{
            BlockHandle, CompressionType, decode_footer, encode_footer,
            encode_index_value_with_props,
        };
        use crate::types::{InternalKey, ValueType};
        use crate::{DB, ErrorKind};
        use std::fs;

        let rechecksum = |raw: &[u8]| -> Vec<u8> {
            let mut out = raw.to_vec();
            out.push(CompressionType::None as u8);
            let crc = crc32fast::hash(&out);
            out.extend_from_slice(&crc.to_le_bytes());
            out
        };

        // Case 1: interior user-key regression ("a"@1, "y"@2, "m"@1, "z"@2).
        // Case 2: same-key ascending sequence inversion ("a"@1, "k"@1->"old", "k"@2->"new", "z"@2).
        for inverted_same_key in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let opts = DbOptions::default()
                .with_l0_compaction_trigger(usize::MAX)
                .with_pin_l0_first_block_and_warm_index(false)
                .with_bloom_bits_per_key(0)
                .with_max_subcompactions(2);
            let db = DB::open(opts.clone(), dir.path()).unwrap();
            db.put(b"a", &[b'v'; 128]).unwrap();
            db.put(b"z", b"vz").unwrap();
            db.close().unwrap();

            let sst_path = fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok().map(|e| e.path()))
                .find(|p| p.extension().is_some_and(|ext| ext == "sst"))
                .unwrap();
            let orig = fs::read(&sst_path).unwrap();
            let (meta_h, _) = decode_footer(orig[orig.len() - 48..].try_into().unwrap()).unwrap();
            let meta_end = (meta_h.offset + meta_h.size + 5) as usize;
            let orig_meta_slice = &orig[meta_h.offset as usize..meta_end];

            let first_ik = InternalKey::new(b"a", 1, ValueType::Value);
            let last_ik = InternalKey::new(b"z", 2, ValueType::Value);
            let mut data_b = BlockBuilder::new(1);
            data_b.add(first_ik.as_bytes(), b"va");
            if inverted_same_key {
                data_b.add(
                    InternalKey::new(b"k", 1, ValueType::Value).as_bytes(),
                    b"old",
                );
                data_b.add(
                    InternalKey::new(b"k", 2, ValueType::Value).as_bytes(),
                    b"new",
                );
            } else {
                data_b.add(
                    InternalKey::new(b"y", 2, ValueType::Value).as_bytes(),
                    b"vy",
                );
                data_b.add(
                    InternalKey::new(b"m", 1, ValueType::Value).as_bytes(),
                    b"vm",
                );
            }
            data_b.add(last_ik.as_bytes(), b"vz");
            let raw_data = data_b.finish();
            let framed_data = rechecksum(&raw_data);

            let data_h = BlockHandle::new(0, raw_data.len() as u64);
            let mut leaf_b = BlockBuilder::new(1);
            let idx_val = encode_index_value_with_props(&data_h, first_ik.as_bytes(), &[]).unwrap();
            leaf_b.add(last_ik.as_bytes(), &idx_val);
            let raw_leaf = leaf_b.finish();
            let framed_leaf = rechecksum(&raw_leaf);

            let leaf_h = BlockHandle::new(framed_data.len() as u64, raw_leaf.len() as u64);
            let new_meta_off = (framed_data.len() + framed_leaf.len()) as u64;
            let new_meta_h = BlockHandle::new(new_meta_off, meta_h.size);

            let mut dir_b = BlockBuilder::new(1);
            dir_b.add(last_ik.as_bytes(), &leaf_h.encode());
            let mut raw_dir = dir_b.finish();
            raw_dir.extend_from_slice(&BlockHandle::default().encode());
            let framed_dir = rechecksum(&raw_dir);
            let dir_off = new_meta_off + orig_meta_slice.len() as u64;
            let dir_h = BlockHandle::new(dir_off, raw_dir.len() as u64);

            let mut image = Vec::new();
            image.extend_from_slice(&framed_data);
            image.extend_from_slice(&framed_leaf);
            image.extend_from_slice(orig_meta_slice);
            image.extend_from_slice(&framed_dir);
            image.extend_from_slice(&encode_footer(&new_meta_h, &dir_h));
            // Update MANIFEST's FileMetaData.file_size? No — pad `meta` or match exact file size?
            // VersionSet::open_table verifies `meta.file_size == reader.file_size()`.
            // Instead of reopening DB from MANIFEST, test execute_compaction_io directly or pad!
            // Padding before `orig_meta_slice` to keep >= original file_size if image < orig.len():
            if image.len() < orig.len() {
                let pad = orig.len() - image.len();
                let mut padded = Vec::with_capacity(orig.len());
                padded.extend_from_slice(&framed_data);
                padded.extend_from_slice(&framed_leaf);
                padded.resize(padded.len() + pad, 0);
                let padded_meta_off = padded.len() as u64;
                padded.extend_from_slice(orig_meta_slice);
                let padded_dir_off = padded.len() as u64;
                padded.extend_from_slice(&framed_dir);
                padded.extend_from_slice(&encode_footer(
                    &BlockHandle::new(padded_meta_off, meta_h.size),
                    &BlockHandle::new(padded_dir_off, raw_dir.len() as u64),
                ));
                image = padded;
            }
            assert_eq!(image.len(), orig.len());
            fs::write(&sst_path, &image).unwrap();

            let db = DB::open(opts, dir.path()).unwrap();
            assert_eq!(db.get(b"k").unwrap_err().kind(), ErrorKind::Corruption);
            let mut scan = db.iter().unwrap();
            while scan.next().is_some() {}
            assert_eq!(scan.error().map(|e| e.kind()), Some(ErrorKind::Corruption));
            db.lazy_delete(b"force_rewrite");
            assert_eq!(db.compact().unwrap_err().kind(), ErrorKind::Corruption);
        }

        // Parallel L1->L2 sub-compaction must also reject unordered restarts (`a, n, o, b, p, z`)
        // when the right worker seeks to split point `m` without visiting `o -> b`.
        {
            use std::sync::{Arc, atomic::Ordering as AtomicOrdering};
            let dir = tempfile::tempdir().unwrap();
            let build_valid = |num: u64, pairs: &[(&[u8], &[u8])]| -> super::TableFile {
                let path = dir.path().join(format!("{num:06}.sst"));
                let mut b = crate::sst::table_builder::TableBuilder::new(
                    &path,
                    crate::sst::table_builder::TableBuildOptions {
                        bloom_bits_per_key: 0,
                        ..Default::default()
                    },
                )
                .unwrap();
                for (uk, v) in pairs {
                    b.add(InternalKey::new(uk, 1, ValueType::Value).as_bytes(), v)
                        .unwrap();
                }
                let res = b.finish().unwrap();
                super::TableFile {
                    meta: crate::manifest::version_edit::FileMetaData {
                        number: num,
                        file_size: res.file_size,
                        smallest_key: res.smallest_key.unwrap(),
                        largest_key: res.largest_key.unwrap(),
                        has_range_deletions: false,
                    },
                    reader: Arc::new(crate::sst::table_reader::TableReader::open(&path).unwrap()),
                }
            };
            let l2_left = build_valid(2, &[(b"a", b"1"), (b"m", b"2")]);
            let l2_right = build_valid(3, &[(b"n", b"3"), (b"z", b"4")]);

            let l1_path = dir.path().join("000001.sst");
            let mut data_b = BlockBuilder::new(1);
            let bad_keys: [&[u8]; 6] = [b"a", b"n", b"o", b"b", b"p", b"z"];
            for uk in bad_keys {
                data_b.add(InternalKey::new(uk, 5, ValueType::Value).as_bytes(), b"v");
            }
            let raw_data = data_b.finish();
            let framed_data = rechecksum(&raw_data);

            let first_ik = InternalKey::new(b"a", 5, ValueType::Value);
            let last_ik = InternalKey::new(b"z", 5, ValueType::Value);
            let data_h = BlockHandle::new(0, raw_data.len() as u64);
            let mut leaf_b = BlockBuilder::new(1);
            let idx_val = encode_index_value_with_props(&data_h, first_ik.as_bytes(), &[]).unwrap();
            leaf_b.add(last_ik.as_bytes(), &idx_val);
            let raw_leaf = leaf_b.finish();
            let framed_leaf = rechecksum(&raw_leaf);

            let mut meta_b = BlockBuilder::new(1);
            meta_b.add(b"props.none", b"");
            let raw_meta = meta_b.finish();
            let framed_meta = rechecksum(&raw_meta);

            let leaf_h = BlockHandle::new(framed_data.len() as u64, raw_leaf.len() as u64);
            let mut dir_b = BlockBuilder::new(1);
            dir_b.add(last_ik.as_bytes(), &leaf_h.encode());
            let mut raw_dir = dir_b.finish();
            raw_dir.extend_from_slice(&BlockHandle::default().encode());
            let framed_dir = rechecksum(&raw_dir);

            let meta_h = BlockHandle::new(
                (framed_data.len() + framed_leaf.len()) as u64,
                raw_meta.len() as u64,
            );
            let dir_h = BlockHandle::new(
                (framed_data.len() + framed_leaf.len() + framed_meta.len()) as u64,
                raw_dir.len() as u64,
            );
            let mut image = Vec::new();
            image.extend_from_slice(&framed_data);
            image.extend_from_slice(&framed_leaf);
            image.extend_from_slice(&framed_meta);
            image.extend_from_slice(&framed_dir);
            image.extend_from_slice(&encode_footer(&meta_h, &dir_h));
            fs::write(&l1_path, &image).unwrap();

            let l1_file = super::TableFile {
                meta: crate::manifest::version_edit::FileMetaData {
                    number: 1,
                    file_size: image.len() as u64,
                    smallest_key: first_ik.into_bytes(),
                    largest_key: last_ik.into_bytes(),
                    has_range_deletions: false,
                },
                reader: Arc::new(crate::sst::table_reader::TableReader::open(&l1_path).unwrap()),
            };
            let task = super::CompactionTask {
                level: 1,
                target_level: 2,
                input_files_level: vec![l1_file],
                input_files_next: vec![l2_left, l2_right],
                deeper_disjoint_levels: None,
            };
            let options = DbOptions {
                max_subcompactions: 2,
                ..Default::default()
            };
            let dir_sync = super::Mutex::new(Some(super::File::open(dir.path()).unwrap()));
            let ctx = super::CompactionContext {
                dir_sync: &dir_sync,
                manifest_poisoned: &super::AtomicBool::new(false),
                db_path: dir.path(),
                table_cache: None,
                options: &options,
                builds: &super::SstBuilds::default(),
                rate_limiter: None,
                stats: None,
                active_snapshots: &[],
            };
            let before = super::PARALLEL_SUB_COMPACTIONS_TAKEN.load(AtomicOrdering::Relaxed);
            let err = super::LeveledCompaction::execute_compaction_io(
                &ctx,
                &task,
                &FileNumbers::new(10),
                true,
            )
            .err()
            .expect("parallel sub-compaction must fail");
            let after = super::PARALLEL_SUB_COMPACTIONS_TAKEN.load(AtomicOrdering::Relaxed);
            assert!(after > before, "must exercise parallel sub-compaction");
            assert_eq!(err.kind(), ErrorKind::Corruption);
        }
    }

    #[test]
    fn pick_compaction_prioritizes_l0_then_highest_score_and_min_overlap_file() {
        use super::{HashSet, VersionEdit, VersionSet};
        use crate::manifest::version_edit::FileMetaData;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let make_meta = |num: u64, lo: &[u8], hi: &[u8], pad_bytes: usize| -> FileMetaData {
            let path = dir.path().join(format!("{num:06}.sst"));
            let mut b = TableBuilder::new(
                &path,
                TableBuildOptions {
                    compression: crate::CompressionType::None,
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            let val = vec![b'x'; pad_bytes];
            b.add(InternalKey::new(lo, 10, ValueType::Value).as_bytes(), &val)
                .unwrap();
            if hi != lo {
                b.add(InternalKey::new(hi, 10, ValueType::Value).as_bytes(), &val)
                    .unwrap();
            }
            let res = b.finish().unwrap();
            FileMetaData {
                number: num,
                file_size: res.file_size,
                smallest_key: res.smallest_key.unwrap(),
                largest_key: res.largest_key.unwrap(),
                has_range_deletions: false,
            }
        };

        let opts = DbOptions::default()
            .with_num_levels(4)
            .with_l0_compaction_trigger(2)
            .with_max_bytes_for_level_base(10_000)
            .with_max_bytes_for_level_multiplier(2.0); // L1 budget = 10 KB, L2 budget = 20 KB

        let mut vs = VersionSet::create(dir.path(), 4).unwrap();
        let mut edit = VersionEdit::new();
        // L0 has 2 files (at trigger)
        edit.add_file(0, make_meta(10, b"a", b"z", 1024));
        edit.add_file(0, make_meta(11, b"a", b"z", 1024));
        // L1 has ~12 KB (score ~1.2)
        edit.add_file(1, make_meta(12, b"a", b"m", 6_000));
        // L2 has ~60 KB across two files (score ~3.0 > L1 score ~1.2):
        // - file 20 (`["a", "f"]`) overlaps 50 KB in L3
        // - file 21 (`["n", "z"]`) has ZERO overlap in L3 (trivial-move candidate!)
        edit.add_file(2, make_meta(20, b"a", b"f", 15_000));
        edit.add_file(2, make_meta(21, b"n", b"z", 15_000));
        edit.add_file(3, make_meta(30, b"a", b"g", 25_000));
        vs.log_and_apply(edit).unwrap();
        let version = vs.current();

        // 1. When L0 >= trigger and unclaimed, L0 -> L1 wins first.
        let t0 = super::LeveledCompaction::pick_compaction(&version, &opts, &HashSet::new())
            .expect("must pick L0");
        assert_eq!(t0.level, 0);

        // 2. When L0 is already claimed in-flight, L2 (score ~3.0) beats L1 (score ~1.2),
        //    and within L2 file 21 (0 overlap in L3) beats file 20 (heavy overlap in L3).
        let in_flight: HashSet<u64> = [10u64].into_iter().collect();
        let t2 = super::LeveledCompaction::pick_compaction(&version, &opts, &in_flight)
            .expect("must pick L2");
        assert_eq!(t2.level, 2);
        assert_eq!(t2.input_files_level.len(), 1);
        assert_eq!(t2.input_files_level[0].meta.number, 21);
        assert!(t2.input_files_next.is_empty());
    }

    #[test]
    fn pick_level_compaction_scales_linearly_on_many_disjoint_files() {
        use super::{HashSet, PICKER_FILE_VISITS, VersionEdit, VersionSet};
        use crate::manifest::version_edit::FileMetaData;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let mut vs = VersionSet::create(path, 3).unwrap();
        let mut edit = VersionEdit::new();
        for i in 0..250u64 {
            let num = 10 + i;
            let sst_path = path.join(format!("{num:06}.sst"));
            let mut b = TableBuilder::new(
                &sst_path,
                TableBuildOptions {
                    compression: crate::CompressionType::None,
                    bloom_bits_per_key: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            let k = format!("k_{i:05}");
            b.add(
                InternalKey::new(k.as_bytes(), 1, ValueType::Value).as_bytes(),
                b"v",
            )
            .unwrap();
            let res = b.finish().unwrap();
            edit.add_file(
                1,
                FileMetaData {
                    number: num,
                    file_size: res.file_size,
                    smallest_key: res.smallest_key.unwrap(),
                    largest_key: res.largest_key.unwrap(),
                    has_range_deletions: false,
                },
            );
        }
        vs.log_and_apply(edit).unwrap();
        let version = vs.current();
        let opts = DbOptions::default()
            .with_num_levels(3)
            .with_max_bytes_for_level_base(1);

        PICKER_FILE_VISITS.with(|n| n.set(0));
        let task = super::LeveledCompaction::pick_compaction(&version, &opts, &HashSet::new())
            .expect("must pick L1");
        let visits = PICKER_FILE_VISITS.with(|n| n.get());
        assert_eq!(task.level, 1);
        assert_eq!(task.input_files_level.len(), 1);
        assert!(
            visits <= 500,
            "expected linear O(F) file visits for 250 disjoint files, got {visits}"
        );
    }

    #[test]
    fn subcompaction_spawn_failure_returns_io_error_and_cleans_staged_outputs() {
        use super::{FAIL_SUB_COMPACTION_SPAWN_AT, HashSet};
        use crate::{DB, ErrorKind};
        use std::{error::Error as _, fs, io};

        let dir = tempfile::tempdir().unwrap();
        // Keep the inputs below both background compaction triggers so a retry
        // cannot create its own SSTs while we check the failed job's cleanup.
        let opts = DbOptions::default()
            .with_num_levels(3)
            .with_max_subcompactions(2)
            .with_max_bytes_for_level_base(u64::MAX)
            .with_l0_compaction_trigger(usize::MAX);
        let db = DB::open(opts, dir.path()).unwrap();

        assert_eq!(db.ingest_sorted([(b"a0", b"1"), (b"m0", b"2")]).unwrap(), 2);
        assert_eq!(db.ingest_sorted([(b"n0", b"3"), (b"z0", b"4")]).unwrap(), 2);
        db.put(b"b0", &[b'x'; 256]).unwrap();
        db.put(b"y0", &[b'y'; 256]).unwrap();
        db.flush().unwrap();

        let sst_paths = || {
            fs::read_dir(dir.path())
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "sst"))
                .collect::<HashSet<_>>()
        };
        let input_ssts = sst_paths();
        assert_eq!(input_ssts.len(), 3, "expected one L0 SST and two L2 SSTs");

        FAIL_SUB_COMPACTION_SPAWN_AT.with(|f| f.set(Some(1)));
        let err = db.compact_range(Some(b"a"), None).unwrap_err();
        FAIL_SUB_COMPACTION_SPAWN_AT.with(|f| f.set(None));

        assert_eq!(err.kind(), ErrorKind::Io);
        let io_kind = err
            .source()
            .and_then(|s| s.downcast_ref::<io::Error>())
            .map(io::Error::kind);
        assert_eq!(io_kind, Some(io::ErrorKind::WouldBlock));

        // The L0 to L1 trivial move keeps its SST. The failed L1 to L2 merge
        // must unlink the first subcompaction's output and preserve every input.
        assert_eq!(sst_paths(), input_ssts);

        for (k, expected) in [
            (b"a0".as_slice(), b"1".as_slice()),
            (b"b0", &[b'x'; 256]),
            (b"m0", b"2"),
            (b"n0", b"3"),
            (b"y0", &[b'y'; 256]),
            (b"z0", b"4"),
        ] {
            assert_eq!(db.get(k).unwrap().as_deref(), Some(expected));
        }

        db.compact_range(Some(b"a"), None).unwrap();
        assert_eq!(db.get(b"b0").unwrap().as_deref(), Some(&[b'x'; 256][..]));
        db.close().unwrap();
    }
}

#[cfg(test)]
thread_local! {
    static LEVEL_BUDGET_MULS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PICKER_FILE_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static FAIL_SUB_COMPACTION_SPAWN_AT: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}
