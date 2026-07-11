//! Leveled compaction strategy.
//!
//! L0 → L1: merge all overlapping L0 files with overlapping L1 files.
//! Ln → Ln+1: pick a file from Ln, merge with overlapping files in Ln+1.
//!
//! After compaction, the old files are deleted and new files are installed.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::fs::remove_file;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread::scope;

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

use crate::cache::block_cache::BlockCache;
use crate::cache::table_cache::TableCache;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::merge::{IterSource, MergingIterator};
use crate::iterator::range_del::RangeTombstoneTracker;
use crate::manifest::version::{TableFile, Version};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::manifest::version_set::VersionSet;
use crate::options::{CompactionFilterDecision, DbOptions};
use crate::rate_limiter::RateLimiter;
use crate::sst::table_builder::{META_BLOCK_SPLIT_THRESHOLD, TableBuildOptions, TableBuilder};
use crate::sst::table_reader::{MAX_DECOMPRESSED_BLOCK_SIZE, TableIterator};
use crate::stats::DbStats;
use crate::types::{
    InternalKey, InternalKeyRef, LazyValue, MAX_SEQUENCE_NUMBER, SequenceNumber, ValueType,
    compare_internal_key, user_key,
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
    pub options: &'a DbOptions,
    pub rate_limiter: Option<&'a Arc<RateLimiter>>,
    pub stats: Option<&'a Arc<DbStats>>,
    pub active_snapshots: &'a [SequenceNumber],
}

/// Description of a compaction to perform.
pub struct CompactionTask {
    /// Source level (files to compact from).
    pub level: usize,
    /// Files from the source level.
    pub input_files_level: Vec<TableFile>,
    /// Files from the target level (level + 1).
    pub input_files_next: Vec<TableFile>,
}

/// Range tombstone user-key extents `[begin, end)` written to each output
/// SST, keyed by file number. Captured from `TableBuildResult` during the
/// unlocked I/O phase so the install-phase overlap precheck never has to
/// open or read an SST while the caller holds the DB lock.
type OutputTombstones = HashMap<u64, Vec<(Vec<u8>, Vec<u8>)>>;

/// Result of the I/O phase of compaction (no lock needed to produce this).
pub struct CompactionOutput {
    /// The version edit with new files added and old files deleted.
    pub edit: VersionEdit,
    /// Exact input files and levels the output was built from.
    pub input_files: Vec<(u32, FileMetaData)>,
    /// File numbers of all input files (for cache eviction and deletion).
    pub input_file_numbers: HashSet<u64>,
    /// One past the highest file number consumed during I/O.
    /// Used to ensure VersionSet::next_file_number doesn't fall behind
    /// the actual numbers used by sub-compaction threads.
    pub next_file_number_hint: u64,
    /// Range tombstone extents per output file, for the install precheck.
    output_tombstones: OutputTombstones,
}

/// Deferred cleanup actions that must be performed AFTER the manifest is
/// synced to disk. Executing these before sync risks data loss on crash.
pub struct PostCompactionCleanup {
    /// Old SST file numbers to delete from disk.
    pub files_to_delete: HashSet<u64>,
}

impl CompactionTask {}

pub struct LeveledCompaction;

/// Collect range tombstones from input files and return them as sorted
/// internal-key entries suitable for injection into the merge iterator.
/// This ensures tombstones from new-format SSTs (which store range
/// deletions in a separate block) participate in the merge.
fn collect_range_del_entries(files: &[TableFile]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut entries = Vec::new();
    for tf in files {
        if tf.meta.has_range_deletions {
            let tombstones = tf.reader.get_range_tombstones().ctx()?;
            for (begin, end, seq) in tombstones {
                let ikey = InternalKey::new(&begin, seq, ValueType::RangeDeletion);
                entries.push((ikey.as_bytes().to_vec(), end));
            }
        }
    }
    entries.sort_by(|a, b| compare_internal_key(&a.0, &b.0));
    Ok(entries)
}

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
    let file_smallest = user_key(&tf.meta.smallest_key);
    let file_largest = user_key(&tf.meta.largest_key);
    let above_lower = lower.is_none_or(|lo| file_largest >= lo);
    let below_upper = upper.is_none_or(|hi| file_smallest < hi);
    above_lower && below_upper
}

fn tombstone_overlaps_bounds(
    begin: &[u8],
    end: &[u8],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> bool {
    let above_lower = lower.is_none_or(|lo| end > lo);
    let below_upper = upper.is_none_or(|hi| begin < hi);
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
    match tf.reader.get_range_tombstones() {
        Ok(tombstones) => tombstones
            .iter()
            .any(|(begin, end, _)| tombstone_overlaps_bounds(begin, end, lower, upper)),
        Err(e) => {
            tracing::warn!(
                "failed to read range tombstones from SST {} while picking compact_range: {}",
                tf.meta.number,
                e
            );
            true
        }
    }
}

fn add_file_extents(tf: &TableFile, extents: &mut Vec<UserKeyRange>) -> bool {
    if !tf.meta.smallest_key.is_empty() && !tf.meta.largest_key.is_empty() {
        extents.push(UserKeyRange::File {
            smallest: user_key(&tf.meta.smallest_key).to_vec(),
            largest: user_key(&tf.meta.largest_key).to_vec(),
        });
    }
    if tf.meta.has_range_deletions {
        match tf.reader.get_range_tombstones() {
            Ok(tombstones) => {
                extents.extend(
                    tombstones
                        .into_iter()
                        .map(|(begin, end, _)| UserKeyRange::Tombstone { begin, end }),
                );
            }
            Err(e) => {
                tracing::warn!(
                    "failed to read range tombstones from SST {} while expanding compaction range: {}",
                    tf.meta.number,
                    e
                );
                return false;
            }
        }
    }
    true
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
    match tf.reader.get_range_tombstones() {
        Ok(tombstones) => tombstones
            .iter()
            .any(|(begin, end, _)| tombstone_overlaps_extent(begin, end, extent)),
        Err(e) => {
            tracing::warn!(
                "failed to read range tombstones from SST {} while checking overlap: {}",
                tf.meta.number,
                e
            );
            true
        }
    }
}

fn overlapping_files_for_inputs(files: &[TableFile], inputs: &[TableFile]) -> Vec<TableFile> {
    let mut extents = Vec::new();
    for tf in inputs {
        if !add_file_extents(tf, &mut extents) {
            return files.to_vec();
        }
    }

    // Seed with the aggregate [min smallest, max largest] range across ALL
    // inputs, not just per-file extents. The merged compaction output may span
    // key gaps between discontiguous inputs (output files are cut by size, not
    // at input-file boundaries), so a target-level file sitting inside such a
    // gap MUST be included as an input — otherwise the installed output would
    // overlap it, breaking the L1+ disjointness invariant and making keys
    // unreachable via binary search. This mirrors `total_key_range` used by
    // the background pickers.
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

    let mut selected = Vec::new();
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
                selected.push(tf.clone());
                if !add_file_extents(tf, &mut extents) {
                    return files.to_vec();
                }
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    selected
}

fn estimated_uncompressed_file_size(tf: &TableFile) -> u64 {
    let data_blocks = tf
        .reader
        .cached_index_entries()
        .map(|entries| entries.len() as u64)
        .unwrap_or_else(|e| {
            tracing::warn!(
                "failed to parse index entries from SST {} while estimating compaction outputs: {}",
                tf.meta.number,
                e
            );
            1
        });
    let data_bound = data_blocks.saturating_mul(MAX_DECOMPRESSED_BLOCK_SIZE as u64);
    let range_del_bound = if tf.meta.has_range_deletions {
        MAX_DECOMPRESSED_BLOCK_SIZE as u64
    } else {
        0
    };
    data_bound
        .saturating_add(range_del_bound)
        .max(tf.meta.file_size)
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
    build_opts: &'a TableBuildOptions,
    oldest_snapshot_seq: SequenceNumber,
    file_number_counter: &'a AtomicU64,
    file_number_limit: u64,
    all_range_del_entries: &'a [(Vec<u8>, Vec<u8>)],
    all_raw_tombstones: &'a [(Vec<u8>, Vec<u8>, SequenceNumber)],
}

/// Output of a single sub-compaction (new files only; deletions handled by orchestrator).
struct SubCompactionOutput {
    new_files: Vec<(u32, FileMetaData)>,
    /// Range tombstone extents per output file (see `OutputTombstones`).
    output_tombstones: OutputTombstones,
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
    let boundaries: Vec<Vec<u8>> = next_files[..next_files.len() - 1]
        .iter()
        .map(|tf| user_key(&tf.meta.largest_key).to_vec())
        .collect();

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
type RawTombstone = (Vec<u8>, Vec<u8>, SequenceNumber);

fn cleanup_output_files(
    db_path: &Path,
    new_files: &[(u32, FileMetaData)],
    active_file_number: Option<u64>,
) {
    for (_, meta) in new_files {
        let orphan = db_path.join(format!("{:06}.sst", meta.number));
        let _ = remove_file(&orphan);
    }
    if let Some(number) = active_file_number {
        let orphan = db_path.join(format!("{:06}.sst", number));
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

fn allocate_output_file_number(params: &SubCompactionParams<'_>) -> Result<u64> {
    let mut current = params.file_number_counter.load(Ordering::Relaxed);
    loop {
        if current >= params.file_number_limit {
            return Err(Error::background(format!(
                "reserved compaction output file numbers exhausted: next={}, limit={}",
                current, params.file_number_limit
            )));
        }
        let next = current
            .checked_add(1)
            .ok_or_else(|| Error::background("compaction output file number overflow"))?;
        match params.file_number_counter.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(current),
            Err(actual) => current = actual,
        }
    }
}

fn collect_raw_tombstones(files: &[TableFile]) -> Result<Vec<RawTombstone>> {
    let mut tombstones = Vec::new();
    for tf in files {
        if tf.meta.has_range_deletions {
            let ts = tf.reader.get_range_tombstones().ctx()?;
            tombstones.extend(ts);
        }
    }
    Ok(tombstones)
}

/// Execute a single sub-compaction covering [lower_bound, upper_bound).
/// File numbers are allocated from a shared atomic counter to avoid collisions.
/// `all_range_del_entries` contains range tombstone merge entries from ALL input files.
/// `all_raw_tombstones` contains raw (begin, end, seq) triples to pre-populate
/// the RangeTombstoneTracker, ensuring tombstones whose start key is before
/// this sub-task's lower_bound are still applied.
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
        sources.push(IterSource::from_boxed(Box::new(iter)));
    }
    for tf in &sub.input_files_next {
        // Compaction scans every input block exactly once; filling the
        // block cache would evict hot point-read blocks for no benefit.
        let iter = TableIterator::new(tf.reader.clone()).with_fill_cache(false);
        sources.push(IterSource::from_boxed(Box::new(iter)));
    }

    // Inject ALL range tombstones into merge stream
    if !params.all_range_del_entries.is_empty() {
        sources.push(IterSource::new(params.all_range_del_entries.to_vec()));
    }

    let mut merger = MergingIterator::new(sources, compare_internal_key);

    // If lower_bound is set, seek past it
    if let Some(ref lo) = sub.lower_bound {
        let seek_key = InternalKey::new(lo, MAX_SEQUENCE_NUMBER, ValueType::Value)
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
    let mut pending_cut = false;
    let mut last_point_key: Option<Vec<u8>> = None;
    let mut last_range_del_key: Option<Vec<u8>> = None;
    let mut last_written_seq: SequenceNumber = 0;
    let mut snapshot_idx: usize = ctx.active_snapshots.len();
    let mut range_tombstones = RangeTombstoneTracker::new();
    // Pre-populate tracker with ALL tombstones so that tombstones whose
    // start key is before this sub-task's lower_bound still take effect.
    for (begin, end, seq) in params.all_raw_tombstones {
        range_tombstones.add(begin.clone(), end.clone(), *seq);
    }
    range_tombstones.reset();

    while let Some((ikey, value)) = merger.next_entry() {
        if ikey.len() < 8 {
            continue;
        }
        let ikr = InternalKeyRef::new(&ikey);
        let user_key = ikr.user_key();

        // Check upper bound: stop if user_key >= upper_bound
        if let Some(ref hi) = sub.upper_bound
            && user_key >= hi.as_slice()
        {
            break;
        }

        // Only cut output files at a user-key boundary: all versions of one user
        // key must stay in the same file, otherwise L1+ files would have
        // overlapping key ranges and a point read could pick the wrong file and
        // miss a visible version. A size-triggered cut is deferred until the user
        // key changes here.
        if pending_cut && builder.is_some() && user_key != current_file_user_key.as_slice() {
            let result = match builder.take().unwrap().finish().ctx() {
                Ok(result) => result,
                Err(e) => {
                    cleanup_output_files(ctx.db_path, &new_files, Some(current_file_number));
                    return Err(e);
                }
            };
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

        if ikr.value_type() == ValueType::RangeDeletion {
            // Not re-added to `range_tombstones` here: `all_raw_tombstones`
            // already pre-populated the tracker with every tombstone in the
            // input set (including this one) before the loop started, so
            // re-adding it on every native occurrence would just duplicate
            // entries and force a full re-sort per tombstone for no benefit.
            if let Some(ref last) = last_range_del_key
                && last.as_slice() == ikey.as_slice()
            {
                continue;
            }
            last_range_del_key = Some(ikey.clone());
            if params.is_bottommost && ikr.sequence() < params.oldest_snapshot_seq {
                continue;
            }
        } else if let Some(ref last) = last_point_key
            && last.as_slice() == user_key
        {
            while snapshot_idx > 0 && ctx.active_snapshots[snapshot_idx - 1] >= last_written_seq {
                snapshot_idx -= 1;
            }
            if snapshot_idx > 0 && ctx.active_snapshots[snapshot_idx - 1] >= ikr.sequence() {
                last_written_seq = ikr.sequence();
                // A retained older version that is shadowed by a range tombstone
                // below the oldest snapshot must still be dropped. Otherwise, if
                // that tombstone is itself dropped at the bottommost level, the
                // value would resurrect for the snapshot that retained it. (The
                // newest-version branch below already applies this check; retained
                // versions need it too.)
                if ikr.value_type() == ValueType::Value
                    && !range_tombstones.is_empty()
                    && range_tombstones.is_deleted(
                        user_key,
                        ikr.sequence(),
                        params.oldest_snapshot_seq,
                    )
                {
                    continue;
                }
            } else {
                continue;
            }
        } else {
            last_point_key = Some(user_key.to_vec());
            last_written_seq = ikr.sequence();
            snapshot_idx = ctx.active_snapshots.len();

            // Only a genuine Deletion may be dropped here, and only at the
            // bottommost level below the oldest snapshot — use the strict
            // decode so a corrupted trailer byte fails the compaction loudly
            // instead of being silently misread as Deletion and permanently
            // destroyed (the original input files are untouched on error).
            if params.is_bottommost
                && ikr.sequence() < params.oldest_snapshot_seq
                && ikr.value_type_checked().ctx()? == ValueType::Deletion
            {
                continue;
            }

            if ikr.value_type() == ValueType::Value && !range_tombstones.is_empty() {
                let entry_seq = ikr.sequence();
                if range_tombstones.is_deleted(user_key, entry_seq, params.oldest_snapshot_seq) {
                    continue;
                }
            }
        }

        // Apply compaction filter
        let mut final_value = value;
        if params.is_bottommost
            && ctx.active_snapshots.is_empty()
            && let Some(ref filter) = ctx.options.compaction_filter
            && ikr.value_type() == ValueType::Value
        {
            match filter.filter(params.target_level, user_key, final_value.as_slice()) {
                CompactionFilterDecision::Keep => {}
                CompactionFilterDecision::Remove => continue,
                CompactionFilterDecision::ChangeValue(new_val) => {
                    final_value = LazyValue::Inline(new_val);
                }
            }
        }

        // Create new output file if needed
        if builder.is_none() {
            current_file_number = match allocate_output_file_number(params).ctx() {
                Ok(number) => number,
                Err(e) => {
                    cleanup_output_files(ctx.db_path, &new_files, None);
                    return Err(e);
                }
            };
            let sst_path = ctx.db_path.join(format!("{:06}.sst", current_file_number));
            let mut opts = params.build_opts.clone();
            opts.block_property_collectors = ctx
                .options
                .block_property_collectors
                .iter()
                .map(|f| f())
                .collect();
            builder = match TableBuilder::new(&sst_path, opts).ctx() {
                Ok(builder) => Some(builder),
                Err(e) => {
                    cleanup_output_files(ctx.db_path, &new_files, Some(current_file_number));
                    return Err(e);
                }
            };
            current_size = 0;
        }

        let final_ikey;
        let ikey_ref = if params.is_bottommost
            && ikr.sequence() > 0
            && ikr.sequence() < params.oldest_snapshot_seq
            && ikr.value_type() == ValueType::Value
        {
            final_ikey = InternalKey::new(user_key, 0, ikr.value_type())
                .as_bytes()
                .to_vec();
            &final_ikey
        } else {
            &ikey
        };

        let entry_bytes = ikey_ref.len() + final_value.len();
        if let Err(e) = builder
            .as_mut()
            .unwrap()
            .add(ikey_ref, final_value.as_slice())
            .ctx()
        {
            cleanup_output_files(ctx.db_path, &new_files, Some(current_file_number));
            return Err(e);
        }
        current_size += entry_bytes;
        if current_file_user_key != user_key {
            current_file_user_key.clear();
            current_file_user_key.extend_from_slice(user_key);
        }

        if let Some(rl) = ctx.rate_limiter {
            rl.request(entry_bytes);
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

    // Flush remaining builder
    let active_file_number = builder.as_ref().map(|_| current_file_number);
    if let Some(b) = builder {
        let result = match b.finish().ctx() {
            Ok(result) => result,
            Err(e) => {
                cleanup_output_files(ctx.db_path, &new_files, Some(current_file_number));
                return Err(e);
            }
        };
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
        // Clean up orphan SST files written during the failed merge
        cleanup_output_files(ctx.db_path, &new_files, active_file_number);
        return Err(Error::background(format!(
            "sub-compaction merge error: {}",
            e
        )));
    }

    Ok(SubCompactionOutput {
        new_files,
        output_tombstones,
    })
}

impl LeveledCompaction {
    pub(crate) fn max_output_files(task: &CompactionTask, options: &DbOptions) -> u64 {
        // Output files are also cut when projected single-block metadata
        // reaches META_BLOCK_SPLIT_THRESHOLD (key-heavy data), so
        // size the reservation for the smaller of the two cut conditions.
        let target_size = options
            .target_file_size_base
            .max(1)
            .min((META_BLOCK_SPLIT_THRESHOLD / 2) as u64);
        let estimated_input_size = task
            .input_files_level
            .iter()
            .chain(task.input_files_next.iter())
            .fold(0u64, |acc, tf| {
                acc.saturating_add(estimated_uncompressed_file_size(tf))
            });
        let size_outputs = estimated_input_size.saturating_add(target_size - 1) / target_size;
        let input_count = task
            .input_files_level
            .len()
            .saturating_add(task.input_files_next.len()) as u64;
        size_outputs
            .saturating_add(input_count)
            .saturating_add(options.max_subcompactions.max(1) as u64)
            .saturating_add(16)
            .max(1)
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
        // Priority 1: L0 → L1 when L0 has too many files
        if version.l0_file_count() >= options.l0_compaction_trigger
            && let Some(task) = Self::pick_l0_compaction(version, in_flight)
        {
            return Some(task);
        }

        // Priority 2: Check each level for size overflow
        for level in 1..version.num_levels - 1 {
            let level_size: u64 = version
                .level_files(level)
                .iter()
                .map(|f| f.meta.file_size)
                .sum();
            let max_size = Self::max_bytes_for_level(options, level);
            if level_size > max_size
                && let Some(task) = Self::pick_level_compaction(version, level, in_flight)
            {
                return Some(task);
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
    fn pick_l0_compaction(version: &Version, in_flight: &HashSet<u64>) -> Option<CompactionTask> {
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

        // Find the total key range of L0 files
        let (smallest, largest) = Self::total_key_range(&input_l0);

        // Find overlapping L1 files
        let input_l1 = if input_l0.iter().any(|tf| tf.meta.has_range_deletions) {
            version.level_files(1).to_vec()
        } else {
            Self::overlapping_files(version.level_files(1), &smallest, &largest)
        };
        if input_l1
            .iter()
            .any(|tf| in_flight.contains(&tf.meta.number))
        {
            return None;
        }

        Some(CompactionTask {
            level: 0,
            input_files_level: input_l0,
            input_files_next: input_l1,
        })
    }

    /// Pick Ln → Ln+1 compaction. Returns `None` (deferring to another
    /// priority/level) if any range-overlapping source or target file is
    /// already claimed by another in-flight compaction.
    fn pick_level_compaction(
        version: &Version,
        level: usize,
        in_flight: &HashSet<u64>,
    ) -> Option<CompactionTask> {
        let files = version.level_files(level);
        if files.is_empty() {
            return None;
        }

        // Pick the largest file in this level that isn't already claimed.
        let target = files
            .iter()
            .filter(|f| !in_flight.contains(&f.meta.number))
            .max_by_key(|f| f.meta.file_size)?;
        // A range tombstone's end key is not represented by the file's
        // smallest/largest metadata. Close the source inputs transitively over
        // real point and tombstone extents so no covering sibling remains
        // behind while a selected value is moved deeper and sequence-zeroed.
        let input_level = overlapping_files_for_inputs(files, std::slice::from_ref(target));
        if input_level
            .iter()
            .any(|tf| in_flight.contains(&tf.meta.number))
        {
            return None;
        }

        let next_level = level + 1;
        let input_next = if next_level < version.num_levels {
            overlapping_files_for_inputs(version.level_files(next_level), &input_level)
        } else {
            Vec::new()
        };
        if input_next
            .iter()
            .any(|tf| in_flight.contains(&tf.meta.number))
        {
            return None;
        }

        Some(CompactionTask {
            level,
            input_files_level: input_level,
            input_files_next: input_next,
        })
    }

    /// Pick compaction for a specific key range. Collects files overlapping
    /// [begin, end) at each level and compacts them.
    pub fn pick_compaction_for_range(
        version: &Version,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Option<CompactionTask> {
        // Check L0 first: collect L0 files overlapping the range
        let l0_files = version.level_files(0);
        let mut input_l0: Vec<TableFile> = Vec::new();
        for tf in l0_files {
            if file_overlaps_compact_bounds(tf, begin, end) {
                input_l0.push(tf.clone());
            }
        }

        if !input_l0.is_empty() {
            let input_l1 = overlapping_files_for_inputs(version.level_files(1), &input_l0);
            return Some(CompactionTask {
                level: 0,
                input_files_level: input_l0,
                input_files_next: input_l1,
            });
        }

        // Check L1+ levels
        for level in 1..version.num_levels - 1 {
            let files = version.level_files(level);
            let mut input_level: Vec<TableFile> = Vec::new();
            for tf in files {
                if file_overlaps_compact_bounds(tf, begin, end) {
                    input_level.push(tf.clone());
                }
            }

            if !input_level.is_empty() {
                let next_level = level + 1;
                let input_next = if next_level < version.num_levels {
                    overlapping_files_for_inputs(version.level_files(next_level), &input_level)
                } else {
                    Vec::new()
                };
                return Some(CompactionTask {
                    level,
                    input_files_level: input_level,
                    input_files_next: input_next,
                });
            }
        }

        None
    }

    /// Pure I/O phase of compaction — does NOT require any lock.
    ///
    /// Merges input files, writes output SSTs using pre-allocated file numbers,
    /// and returns a `CompactionOutput` ready for `install_compaction`.
    ///
    /// When `options.max_subcompactions > 1` and the target level has enough
    /// files to split on, the work is divided into parallel sub-compactions
    /// using `std::thread::scope`.
    pub(crate) fn execute_compaction_io(
        ctx: &CompactionContext<'_>,
        task: &CompactionTask,
        file_number_start: u64,
        file_number_limit: u64,
        is_bottommost: bool,
    ) -> Result<CompactionOutput> {
        let target_level = task.level + 1;

        // Trivial move optimization: if there's exactly one input file and no
        // overlap with the next level, just move the metadata without
        // rewriting. Skip it when the compaction filter could remove or
        // change entries — it needs to see every key-value pair. A filter
        // that reports itself a no-op (e.g. lazy-delete with no user filter
        // and no registered dead keys) still permits the move. Building this
        // as a normal `CompactionOutput` (rather than applying it directly)
        // means it goes through `install_compaction`'s stale-input check
        // like any other compaction — required now that every caller of
        // this function (including concurrent background workers) can reach
        // this path, not just the single-threaded `do_compaction`.
        if task.input_files_level.len() == 1
            && task.input_files_next.is_empty()
            && ctx
                .options
                .compaction_filter
                .as_ref()
                .is_none_or(|f| f.is_noop())
        {
            let tf = &task.input_files_level[0];
            let mut edit = VersionEdit::new();
            edit.delete_file(task.level as u32, tf.meta.number);
            edit.add_file(target_level as u32, tf.meta.clone());
            return Ok(CompactionOutput {
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
                // No new file numbers are consumed by a metadata-only move.
                next_file_number_hint: file_number_start,
                output_tombstones: OutputTombstones::new(),
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
            bloom_bits_per_key: ctx.options.bloom_bits_per_key,
            internal_keys: true,
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
        // `all_range_del_entries`/`all_raw_tombstones` below are collected
        // once from ALL input files and shared (by reference) with every
        // sub-task's `SubCompactionParams`, so each sub-compaction already
        // gets full, complete tombstone visibility regardless of its own
        // key range — the cross-sub-task sharing this data structure exists
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
        let all_range_del_entries = collect_range_del_entries(&all_input_files).ctx()?;
        let all_raw_tombstones = collect_raw_tombstones(&all_input_files).ctx()?;

        // Shared atomic counter for thread-safe file number allocation
        let file_counter = AtomicU64::new(file_number_start);

        let sub_params = SubCompactionParams {
            target_level,
            is_bottommost,
            build_opts: &build_opts,
            oldest_snapshot_seq,
            file_number_counter: &file_counter,
            file_number_limit,
            all_range_del_entries: &all_range_del_entries,
            all_raw_tombstones: &all_raw_tombstones,
        };

        let sub_outputs = if actual_subs <= 1 {
            // Fast path: single sub-compaction (zero overhead)
            vec![execute_sub_compaction_io(ctx, &sub_tasks[0], &sub_params).ctx()?]
        } else {
            // Parallel sub-compactions
            #[cfg(test)]
            PARALLEL_SUB_COMPACTIONS_TAKEN.fetch_add(1, Ordering::Relaxed);
            let thread_results: Vec<Result<SubCompactionOutput>> = scope(|s| {
                let handles: Vec<_> = sub_tasks
                    .iter()
                    .map(|sub| s.spawn(|| execute_sub_compaction_io(ctx, sub, &sub_params)))
                    .collect();
                handles
                    .into_iter()
                    .map(|h| match h.join() {
                        Ok(r) => r,
                        Err(_) => Err(Error::background("sub-compaction thread panicked")),
                    })
                    .collect()
            });
            let mut outputs = Vec::with_capacity(thread_results.len());
            let mut first_err: Option<Error> = None;
            for r in thread_results {
                match r {
                    Ok(sub_out) => outputs.push(sub_out),
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            if let Some(e) = first_err {
                // Collect known file numbers from successful sub-compactions.
                let known: std::collections::HashSet<u64> = outputs
                    .iter()
                    .flat_map(|o| o.new_files.iter().map(|(_, m)| m.number))
                    .collect();
                // Clean up SST files from successful sub-compactions.
                for sub_out in &outputs {
                    for (_, meta) in &sub_out.new_files {
                        let orphan = ctx.db_path.join(format!("{:06}.sst", meta.number));
                        let _ = remove_file(&orphan);
                    }
                }
                // Clean up any orphaned SSTs from the panicked thread(s):
                // file numbers were consumed from the shared counter but never
                // reported, so delete any .sst file in the allocated range that
                // is not accounted for in the successful outputs.
                let end = file_counter.load(Ordering::Acquire);
                for num in file_number_start..end {
                    if !known.contains(&num) {
                        let orphan = ctx.db_path.join(format!("{:06}.sst", num));
                        let _ = remove_file(&orphan);
                    }
                }
                return Err(e);
            }
            outputs
        };

        // Merge sub-compaction outputs
        let mut edit = VersionEdit::new();
        let mut output_tombstones = OutputTombstones::new();
        for sub_out in sub_outputs {
            for file_entry in sub_out.new_files {
                edit.new_files.push(file_entry);
            }
            // File numbers come from a shared atomic counter, so per-sub maps
            // are disjoint and extend cannot collide.
            output_tombstones.extend(sub_out.output_tombstones);
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

        Ok(CompactionOutput {
            edit,
            input_files,
            input_file_numbers,
            next_file_number_hint: file_counter.load(Ordering::Relaxed),
            output_tombstones,
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
    ) -> Result<PostCompactionCleanup> {
        // Files this compaction physically CREATED. A trivial move's "new"
        // file is the live input file itself (same number, relabeled to the
        // target level), so it must never be deleted or evicted by the
        // discard/error paths below — the current version still references
        // it at the source level, and deleting it would permanently destroy
        // data the MANIFEST points at. Normal outputs use freshly reserved
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
        // outdated version and must be discarded.
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
            stale || Self::outputs_overlap_unexpected_current_files(&output, &version)
        };
        if discard {
            cleanup_output_files(db_path, &created_files, None);
            evict_table_cache_files(table_cache, &created_files);
            return Ok(PostCompactionCleanup {
                files_to_delete: HashSet::new(),
            });
        }

        // Belt-and-braces: `allocate_output_file_number` enforces
        // `file_number_limit`, so the sub-compaction counter cannot exceed
        // the range reserved under the DB lock, and the reservation already
        // advanced `next_file_number` past `file_number_limit` — this call
        // is expected to be a no-op. Keep it as a cheap forward-only guard
        // so file numbers stay collision-free even if a future edge case
        // violates that reasoning.
        versions.ensure_file_number_at_least(output.next_file_number_hint);
        output
            .edit
            .set_next_file_number(versions.next_file_number());
        if let Err(e) = versions.log_and_apply(output.edit) {
            // Delete only the freshly-written SSTs (otherwise they would be
            // orphaned on disk) — `created_files` excludes a trivial move's
            // still-live input file.
            cleanup_output_files(db_path, &created_files, None);
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

    /// Force-merge all files at a given level into one output at the same level.
    /// Drops tombstones if this is the bottommost level.
    pub(crate) fn force_merge_level(
        ctx: &CompactionContext<'_>,
        level: usize,
        versions: &mut VersionSet,
        table_cache: Option<&Arc<TableCache>>,
        block_cache: Option<&Arc<BlockCache>>,
    ) -> Result<()> {
        let oldest_snapshot_seq = ctx
            .active_snapshots
            .iter()
            .min()
            .copied()
            .unwrap_or(SequenceNumber::MAX);
        let version = versions.current();
        let files = version.level_files(level);
        if files.is_empty() {
            return Ok(());
        }
        let is_bottommost =
            Self::is_bottommost_level(&version, level, ctx.options.num_levels, files);
        // A single file with a no-op filter is already in its final form —
        // rewriting would waste I/O for no benefit. The same holds when a
        // non-noop filter cannot fire anyway (the merge loop only applies
        // filters at the bottommost level with no active snapshots): the
        // rewrite would be byte-for-byte identical to its input. Skipping
        // also forgoes snapshot-dedup/seq-zeroing for that file, but that
        // only retains extra data — always read-safe, and picked up by the
        // next compaction that touches the file. When the filter can
        // actually remove or change entries (e.g. lazy-delete dead keys),
        // even a single file must be reprocessed so every key-value pair
        // passes through the filter.
        let filter_can_apply = is_bottommost && ctx.active_snapshots.is_empty();
        if files.len() == 1
            && (!filter_can_apply
                || ctx
                    .options
                    .compaction_filter
                    .as_ref()
                    .is_none_or(|f| f.is_noop()))
        {
            return Ok(());
        }

        let mut sources: Vec<IterSource> = Vec::new();
        for tf in files {
            // Compaction scans every input block exactly once; filling the
            // block cache would evict hot point-read blocks for no benefit.
            let iter = TableIterator::new(tf.reader.clone()).with_fill_cache(false);
            sources.push(IterSource::from_boxed(Box::new(iter)));
        }

        // Inject range tombstones from new-format SSTs into the merge stream
        let range_del_entries = collect_range_del_entries(files).ctx()?;
        if !range_del_entries.is_empty() {
            sources.push(IterSource::new(range_del_entries));
        }

        let mut merger = MergingIterator::new(sources, compare_internal_key);

        let compression = if !ctx.options.compression_per_level.is_empty()
            && level < ctx.options.compression_per_level.len()
        {
            ctx.options.compression_per_level[level]
        } else {
            ctx.options.compression
        };
        // build_opts is a template; block_property_collectors are created fresh
        // per output file (via factory functions) to avoid sharing mutable state.
        let build_opts = TableBuildOptions {
            block_size: ctx.options.block_size,
            block_restart_interval: ctx.options.block_restart_interval,
            bloom_bits_per_key: ctx.options.bloom_bits_per_key,
            internal_keys: true,
            compression,
            prefix_len: ctx.options.prefix_len,
            block_property_collectors: Vec::new(),
        };

        let mut edit = VersionEdit::new();
        let mut builder: Option<TableBuilder> = None;
        let mut current_file_number = 0u64;
        let mut current_size = 0usize;
        let mut current_file_user_key: Vec<u8> = Vec::new();
        let mut pending_cut = false;
        let mut last_point_key: Option<Vec<u8>> = None;
        let mut last_range_del_key: Option<Vec<u8>> = None;
        let mut last_written_seq: SequenceNumber = 0;
        let mut snapshot_idx: usize = ctx.active_snapshots.len();
        let mut range_tombstones = RangeTombstoneTracker::new();

        while let Some((ikey, value)) = merger.next_entry() {
            if ikey.len() < 8 {
                continue;
            }
            let ikr = InternalKeyRef::new(&ikey);
            let user_key = ikr.user_key();

            // Defer size-triggered file cuts to user-key boundaries so a key's
            // versions are never split across files (which would create overlapping
            // same-level key ranges and possibly miss visible versions on reads).
            if pending_cut && builder.is_some() && user_key != current_file_user_key.as_slice() {
                let result = match builder.take().unwrap().finish().ctx() {
                    Ok(result) => result,
                    Err(e) => {
                        cleanup_output_files(
                            ctx.db_path,
                            &edit.new_files,
                            Some(current_file_number),
                        );
                        return Err(e);
                    }
                };
                if let Some(s) = ctx.stats {
                    s.record_compaction_bytes(result.file_size);
                }
                edit.add_file(
                    level as u32,
                    FileMetaData {
                        number: current_file_number,
                        file_size: result.file_size,
                        smallest_key: result.smallest_key.unwrap_or_default(),
                        largest_key: result.largest_key.unwrap_or_default(),
                        has_range_deletions: result.has_range_deletions,
                    },
                );
                current_size = 0;
                pending_cut = false;
            }

            if ikr.value_type() == ValueType::RangeDeletion {
                range_tombstones.add(user_key.to_vec(), value.as_slice().to_vec(), ikr.sequence());
                range_tombstones.reset();
                if let Some(ref last) = last_range_del_key
                    && last.as_slice() == ikey.as_slice()
                {
                    continue;
                }
                last_range_del_key = Some(ikey.clone());
                if is_bottommost && ikr.sequence() < oldest_snapshot_seq {
                    continue;
                }
            } else if let Some(ref last) = last_point_key
                && last.as_slice() == user_key
            {
                // Same key — check if a snapshot needs this version.
                while snapshot_idx > 0 && ctx.active_snapshots[snapshot_idx - 1] >= last_written_seq
                {
                    snapshot_idx -= 1;
                }
                if snapshot_idx > 0 && ctx.active_snapshots[snapshot_idx - 1] >= ikr.sequence() {
                    last_written_seq = ikr.sequence();
                    // A retained older version shadowed by a range tombstone below
                    // the oldest snapshot must still be dropped, or it would
                    // resurrect if that tombstone is dropped at the bottommost level.
                    if ikr.value_type() == ValueType::Value
                        && !range_tombstones.is_empty()
                        && range_tombstones.is_deleted(
                            user_key,
                            ikr.sequence(),
                            oldest_snapshot_seq,
                        )
                    {
                        continue;
                    }
                } else {
                    continue;
                }
            } else {
                last_point_key = Some(user_key.to_vec());
                last_written_seq = ikr.sequence();
                snapshot_idx = ctx.active_snapshots.len();

                // See execute_sub_compaction_io: strict decode so a corrupted
                // trailer byte fails loudly instead of being silently
                // misread as Deletion and permanently destroyed.
                if is_bottommost
                    && ikr.sequence() < oldest_snapshot_seq
                    && ikr.value_type_checked().ctx()? == ValueType::Deletion
                {
                    continue;
                }

                if ikr.value_type() == ValueType::Value && !range_tombstones.is_empty() {
                    let entry_seq = ikr.sequence();
                    if range_tombstones.is_deleted(user_key, entry_seq, oldest_snapshot_seq) {
                        continue;
                    }
                }
            }

            // Apply compaction filter
            let mut final_value = value;
            if is_bottommost
                && ctx.active_snapshots.is_empty()
                && let Some(ref filter) = ctx.options.compaction_filter
                && ikr.value_type() == ValueType::Value
            {
                match filter.filter(level, user_key, final_value.as_slice()) {
                    CompactionFilterDecision::Keep => {}
                    CompactionFilterDecision::Remove => continue,
                    CompactionFilterDecision::ChangeValue(new_val) => {
                        final_value = LazyValue::Inline(new_val);
                    }
                }
            }

            if builder.is_none() {
                current_file_number = versions.new_file_number();
                let sst_path = ctx.db_path.join(format!("{:06}.sst", current_file_number));
                let mut opts = build_opts.clone();
                opts.block_property_collectors = ctx
                    .options
                    .block_property_collectors
                    .iter()
                    .map(|f| f())
                    .collect();
                builder = match TableBuilder::new(&sst_path, opts).ctx() {
                    Ok(builder) => Some(builder),
                    Err(e) => {
                        cleanup_output_files(
                            ctx.db_path,
                            &edit.new_files,
                            Some(current_file_number),
                        );
                        return Err(e);
                    }
                };
                current_size = 0;
            }

            // Sequence zeroing: at the bottommost level, if the entry's
            // sequence falls below the minimum active snapshot, zero it out.
            let final_ikey;
            let ikey_ref = if is_bottommost
                && ikr.sequence() > 0
                && ikr.sequence() < oldest_snapshot_seq
                && ikr.value_type() == ValueType::Value
            {
                final_ikey = InternalKey::new(user_key, 0, ikr.value_type())
                    .as_bytes()
                    .to_vec();
                &final_ikey
            } else {
                &ikey
            };

            if let Err(e) = builder
                .as_mut()
                .unwrap()
                .add(ikey_ref, final_value.as_slice())
                .ctx()
            {
                cleanup_output_files(ctx.db_path, &edit.new_files, Some(current_file_number));
                return Err(e);
            }
            let entry_bytes = ikey_ref.len() + final_value.len();
            current_size += entry_bytes;
            if current_file_user_key != user_key {
                current_file_user_key.clear();
                current_file_user_key.extend_from_slice(user_key);
            }

            // Rate-limit compaction writes
            if let Some(rl) = ctx.rate_limiter {
                rl.request(entry_bytes);
            }

            if current_size >= ctx.options.target_file_size_base as usize
                || builder.as_ref().unwrap().projected_meta_size() >= META_BLOCK_SPLIT_THRESHOLD
            {
                // Defer the actual cut to the next user-key boundary. The
                // meta-size condition keeps each single-block metadata structure
                // well below the reader's hard cap for key-heavy data.
                pending_cut = true;
            }
        }

        let active_file_number = builder.as_ref().map(|_| current_file_number);
        if let Some(b) = builder {
            let result = match b.finish().ctx() {
                Ok(result) => result,
                Err(e) => {
                    cleanup_output_files(ctx.db_path, &edit.new_files, Some(current_file_number));
                    return Err(e);
                }
            };
            if let Some(s) = ctx.stats {
                s.record_compaction_bytes(result.file_size);
            }
            edit.add_file(
                level as u32,
                FileMetaData {
                    number: current_file_number,
                    file_size: result.file_size,
                    smallest_key: result.smallest_key.unwrap_or_default(),
                    largest_key: result.largest_key.unwrap_or_default(),
                    has_range_deletions: result.has_range_deletions,
                },
            );
        }

        // Abort if the merge iterator encountered an I/O or corruption error.
        if let Some(e) = merger.error() {
            // Clean up orphan SST files written during the failed merge
            for (_, meta) in &edit.new_files {
                let orphan = ctx.db_path.join(format!("{:06}.sst", meta.number));
                let _ = remove_file(&orphan);
            }
            if let Some(num) = active_file_number {
                let orphan = ctx.db_path.join(format!("{:06}.sst", num));
                let _ = remove_file(&orphan);
            }
            return Err(Error::background(format!(
                "force_merge iterator error: {}",
                e
            )));
        }

        let input_file_numbers: HashSet<u64> = files.iter().map(|f| f.meta.number).collect();
        for tf in files {
            edit.delete_file(level as u32, tf.meta.number);
        }

        edit.set_next_file_number(versions.next_file_number());
        // Capture output files so they can be deleted if the install fails,
        // otherwise the freshly-written SSTs would be orphaned on disk.
        let output_files = edit.new_files.clone();
        if let Err(e) = versions.log_and_apply(edit) {
            cleanup_output_files(ctx.db_path, &output_files, None);
            return Err(e).ctx();
        }
        // force_merge_level holds &mut VersionSet for the duration, sync here.
        versions.sync_manifest().ctx()?;

        if let Some(cache) = table_cache {
            for num in &input_file_numbers {
                cache.evict(*num);
            }
        }
        if let Some(bc) = block_cache {
            for num in &input_file_numbers {
                bc.invalidate_file(*num);
            }
        }

        // Safe to delete old SSTs after manifest is synced
        for num in &input_file_numbers {
            let old_path = ctx.db_path.join(format!("{:06}.sst", num));
            if let Err(e) = remove_file(&old_path) {
                tracing::warn!("failed to remove old SST {}: {}", old_path.display(), e);
            }
        }

        if let Some(s) = ctx.stats {
            s.record_compaction_completed();
        }

        Ok(())
    }

    /// Pick a compaction based on a read-triggered hint. If the hinted level
    /// has more than one file, pick the largest file for compaction into the
    /// next level.
    pub fn pick_compaction_for_hint(
        version: &Version,
        hint: &CompactionHint,
        in_flight: &HashSet<u64>,
    ) -> Option<CompactionTask> {
        let level = hint.level;
        if level == 0 || level >= version.num_levels.saturating_sub(1) {
            return None;
        }
        let files = version.level_files(level);
        if files.len() <= 1 {
            return None;
        }
        // Pick the largest file at the hinted level.
        Self::pick_level_compaction(version, level, in_flight)
    }

    /// Maximum bytes for a given level.
    fn max_bytes_for_level(options: &DbOptions, level: usize) -> u64 {
        let mut result = options.max_bytes_for_level_base;
        for _ in 1..level {
            result = (result as f64 * options.max_bytes_for_level_multiplier) as u64;
        }
        result
    }

    /// Compute the total key range of a set of files.
    /// Uses `compare_internal_key` for correct variable-length user key ordering.
    fn total_key_range(files: &[TableFile]) -> (Vec<u8>, Vec<u8>) {
        let mut smallest = Vec::new();
        let mut largest = Vec::new();

        for f in files {
            if smallest.is_empty()
                || compare_internal_key(&f.meta.smallest_key, &smallest) == CmpOrdering::Less
            {
                smallest = f.meta.smallest_key.clone();
            }
            if largest.is_empty()
                || compare_internal_key(&f.meta.largest_key, &largest) == CmpOrdering::Greater
            {
                largest = f.meta.largest_key.clone();
            }
        }

        (smallest, largest)
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
            if !add_file_extents(tf, &mut extents) {
                return false;
            }
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

    /// Find files in a level that overlap with the given key range.
    /// Uses **user key** comparison to detect overlap correctly.
    ///
    /// Internal key comparison is wrong here: two files sharing the same user
    /// key at different sequence numbers can appear non-overlapping in internal
    /// key order (higher seq sorts first). For example, an L0 tombstone at
    /// (key, seq=100) sorts *before* an L1 value at (key, seq=0), making the
    /// internal-key ranges disjoint even though both files contain the same
    /// user key.
    fn overlapping_files(files: &[TableFile], smallest: &[u8], largest: &[u8]) -> Vec<TableFile> {
        let smallest_uk = user_key(smallest);
        let largest_uk = user_key(largest);
        files
            .iter()
            .filter(|f| {
                let file_largest_uk = user_key(&f.meta.largest_key);
                let file_smallest_uk = user_key(&f.meta.smallest_key);
                // File overlaps if: file.largest_uk >= smallest_uk AND file.smallest_uk <= largest_uk.
                // This is metadata-only; range-aware callers use `overlapping_files_for_inputs`.
                file_largest_uk >= smallest_uk && file_smallest_uk <= largest_uk
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DB;
    use crate::options::DbOptions;

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
                    internal_keys: true,
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
            edit,
            input_files: vec![(1, meta1.clone())],
            input_file_numbers: HashSet::new(),
            next_file_number_hint: versions.next_file_number(),
            output_tombstones: OutputTombstones::new(),
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
                    internal_keys: true,
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
            input_files_level: vec![source_file],
            input_files_next: target_files,
        };

        let options = DbOptions {
            max_subcompactions: 4,
            ..Default::default()
        };
        let ctx = CompactionContext {
            db_path: dir.path(),
            options: &options,
            rate_limiter: None,
            stats: None,
            active_snapshots: &[],
        };

        let before = PARALLEL_SUB_COMPACTIONS_TAKEN.load(AtomicOrdering::Relaxed);
        let output =
            LeveledCompaction::execute_compaction_io(&ctx, &task, 100, u64::MAX, true).unwrap();
        let after = PARALLEL_SUB_COMPACTIONS_TAKEN.load(AtomicOrdering::Relaxed);

        assert!(
            after > before,
            "execute_compaction_io should have taken the parallel (actual_subs > 1) \
             path given 3 disjoint target-level files and \
             max_subcompactions=4, but the counter did not increment — the \
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
                    internal_keys: true,
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
                input_files_level: vec![source_file],
                input_files_next: target_files,
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
            let ctx = CompactionContext {
                db_path: dir.path(),
                options: &options,
                rate_limiter: None,
                stats: None,
                active_snapshots,
            };

            let before = PARALLEL_SUB_COMPACTIONS_TAKEN.load(AtomicOrdering::Relaxed);
            let output = LeveledCompaction::execute_compaction_io(
                &ctx,
                &task,
                file_number_start,
                u64::MAX,
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
        assert!(
            snapshot_tombstones.contains(&(key(120).into_bytes(), key(220).into_bytes(), 50)),
            "a snapshot below the tombstone sequence must retain the range tombstone"
        );
        assert!(
            snapshot_points.iter().any(|(k, seq, _, v)| {
                k == &key(150) && *seq == 10 && v == &value("source", 150)
            }),
            "covered keys must be retained when a snapshot below the tombstone needs them"
        );
    }
}
