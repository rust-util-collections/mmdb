//! Leveled compaction strategy.
//!
//! L0 → L1: merge all overlapping L0 files with overlapping L1 files.
//! Ln → Ln+1: pick a file from Ln, merge with overlapping files in Ln+1.
//!
//! After compaction, the old files are deleted and new files are installed.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ruc::*;

use crate::cache::table_cache::TableCache;
use crate::error::Result;
use crate::iterator::merge::{IterSource, MergingIterator};
use crate::iterator::range_del::RangeTombstoneTracker;
use crate::manifest::version::{TableFile, Version};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::manifest::version_set::VersionSet;
use crate::options::DbOptions;
use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
use crate::sst::table_reader::TableIterator;
use crate::types::{
    InternalKey, InternalKeyRef, LazyValue, SequenceNumber, ValueType, compare_internal_key,
};

/// A hint from the read path that a specific level may benefit from compaction.
/// Accumulated by the DB and drained by the compaction worker.
#[derive(Debug, Clone)]
pub struct CompactionHint {
    /// The level that was read-hot.
    pub level: usize,
    /// Number of sampled reads at this level.
    pub read_count: u64,
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

/// Result of the I/O phase of compaction (no lock needed to produce this).
pub struct CompactionOutput {
    /// The version edit with new files added and old files deleted.
    pub edit: VersionEdit,
    /// File numbers of all input files (for cache eviction and deletion).
    pub input_file_numbers: HashSet<u64>,
    /// Number of output files produced.
    pub files_produced: u64,
    /// One past the highest file number consumed during I/O.
    /// Used to ensure VersionSet::next_file_number doesn't fall behind
    /// the actual numbers used by sub-compaction threads.
    pub next_file_number_hint: u64,
}

impl CompactionTask {
    /// Total size of all input files (both levels).
    pub fn total_input_size(&self) -> u64 {
        self.input_files_level
            .iter()
            .chain(self.input_files_next.iter())
            .map(|f| f.meta.file_size)
            .sum()
    }
}

pub struct LeveledCompaction;

/// Collect range tombstones from input files and return them as sorted
/// internal-key entries suitable for injection into the merge iterator.
/// This ensures tombstones from new-format SSTs (which store range
/// deletions in a separate block) participate in the merge.
fn collect_range_del_entries(files: &[TableFile]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut entries = Vec::new();
    for tf in files {
        if tf.meta.has_range_deletions {
            let tombstones = tf.reader.get_range_tombstones().c(d!())?;
            for (begin, end, seq) in tombstones {
                let ikey = InternalKey::new(&begin, seq, ValueType::RangeDeletion);
                entries.push((ikey.as_bytes().to_vec(), end));
            }
        }
    }
    entries.sort_by(|a, b| compare_internal_key(&a.0, &b.0));
    Ok(entries)
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

/// Output of a single sub-compaction (new files only; deletions handled by orchestrator).
struct SubCompactionOutput {
    new_files: Vec<(u32, FileMetaData)>,
    files_produced: u64,
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
        .map(|tf| crate::types::user_key(&tf.meta.largest_key).to_vec())
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
                let file_smallest = crate::types::user_key(&tf.meta.smallest_key);
                let file_largest = crate::types::user_key(&tf.meta.largest_key);
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
                    let file_smallest = crate::types::user_key(&tf.meta.smallest_key);
                    let file_largest = crate::types::user_key(&tf.meta.largest_key);
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

fn collect_raw_tombstones(files: &[TableFile]) -> Result<Vec<RawTombstone>> {
    let mut tombstones = Vec::new();
    for tf in files {
        if tf.meta.has_range_deletions {
            let ts = tf.reader.get_range_tombstones().c(d!())?;
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
#[allow(clippy::too_many_arguments)]
fn execute_sub_compaction_io(
    sub: &SubCompactionTask,
    target_level: usize,
    is_bottommost: bool,
    db_path: &Path,
    build_opts: &TableBuildOptions,
    options: &DbOptions,
    rate_limiter: Option<&Arc<crate::rate_limiter::RateLimiter>>,
    stats: Option<&Arc<crate::stats::DbStats>>,
    active_snapshots: &[SequenceNumber],
    min_unflushed_seq: SequenceNumber,
    file_number_counter: &AtomicU64,
    all_range_del_entries: &[(Vec<u8>, Vec<u8>)],
    all_raw_tombstones: &[(Vec<u8>, Vec<u8>, SequenceNumber)],
) -> Result<SubCompactionOutput> {
    // Build streaming merge sources
    let mut sources: Vec<IterSource> = Vec::new();
    for tf in &sub.input_files_level {
        let iter = TableIterator::new(tf.reader.clone());
        sources.push(IterSource::from_boxed(Box::new(iter)));
    }
    for tf in &sub.input_files_next {
        let iter = TableIterator::new(tf.reader.clone());
        sources.push(IterSource::from_boxed(Box::new(iter)));
    }

    // Inject ALL range tombstones into merge stream
    if !all_range_del_entries.is_empty() {
        sources.push(IterSource::new(all_range_del_entries.to_vec()));
    }

    let mut merger = MergingIterator::new(sources, compare_internal_key);

    // If lower_bound is set, seek past it
    if let Some(ref lo) = sub.lower_bound {
        let seek_key = InternalKey::new(lo, crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value)
            .as_bytes()
            .to_vec();
        merger.seek(&seek_key);
    }

    let mut new_files: Vec<(u32, FileMetaData)> = Vec::new();
    let mut builder: Option<TableBuilder> = None;
    let mut current_file_number = 0u64;
    let mut next_file_idx = 0u64;
    let mut current_size = 0usize;
    let mut last_point_key: Option<Vec<u8>> = None;
    let mut last_range_del_key: Option<Vec<u8>> = None;
    let mut last_written_seq: SequenceNumber = 0;
    let mut snapshot_idx: usize = active_snapshots.len();
    let mut range_tombstones = RangeTombstoneTracker::new();
    // Pre-populate tracker with ALL tombstones so that tombstones whose
    // start key is before this sub-task's lower_bound still take effect.
    for (begin, end, seq) in all_raw_tombstones {
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

        if ikr.value_type() == ValueType::RangeDeletion {
            range_tombstones.add(user_key.to_vec(), value.as_slice().to_vec(), ikr.sequence());
            range_tombstones.reset();
            if let Some(ref last) = last_range_del_key
                && last.as_slice() == user_key
            {
                continue;
            }
            last_range_del_key = Some(user_key.to_vec());
            if is_bottommost {
                continue;
            }
        } else if let Some(ref last) = last_point_key
            && last.as_slice() == user_key
        {
            while snapshot_idx > 0 && active_snapshots[snapshot_idx - 1] >= last_written_seq {
                snapshot_idx -= 1;
            }
            if snapshot_idx > 0 && active_snapshots[snapshot_idx - 1] >= ikr.sequence() {
                last_written_seq = ikr.sequence();
            } else {
                continue;
            }
        } else {
            last_point_key = Some(user_key.to_vec());
            last_written_seq = ikr.sequence();
            snapshot_idx = active_snapshots.len();

            if ikr.value_type() == ValueType::Deletion && is_bottommost {
                continue;
            }

            if ikr.value_type() == ValueType::Value && !range_tombstones.is_empty() {
                let entry_seq = ikr.sequence();
                if range_tombstones.is_deleted(user_key, entry_seq, SequenceNumber::MAX) {
                    continue;
                }
            }
        }

        // Apply compaction filter
        let mut final_value = value;
        if let Some(ref filter) = options.compaction_filter
            && ikr.value_type() == ValueType::Value
        {
            use crate::options::CompactionFilterDecision;
            match filter.filter(target_level, user_key, final_value.as_slice()) {
                CompactionFilterDecision::Keep => {}
                CompactionFilterDecision::Remove => continue,
                CompactionFilterDecision::ChangeValue(new_val) => {
                    final_value = LazyValue::Inline(new_val);
                }
            }
        }

        // Create new output file if needed
        if builder.is_none() {
            current_file_number = file_number_counter.fetch_add(1, Ordering::Relaxed);
            next_file_idx += 1;
            let sst_path = db_path.join(format!("{:06}.sst", current_file_number));
            let mut opts = build_opts.clone();
            opts.block_property_collectors = options
                .block_property_collectors
                .iter()
                .map(|f| f())
                .collect();
            builder = Some(TableBuilder::new(&sst_path, opts).c(d!())?);
            current_size = 0;
        }

        let final_ikey;
        let ikey_ref = if is_bottommost
            && ikr.sequence() > 0
            && ikr.sequence() < min_unflushed_seq
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
        builder
            .as_mut()
            .unwrap()
            .add(ikey_ref, final_value.as_slice())
            .c(d!())?;
        current_size += entry_bytes;

        if let Some(rl) = rate_limiter {
            rl.request(entry_bytes);
        }

        if current_size >= options.target_file_size_base as usize {
            let result = builder.take().unwrap().finish().c(d!())?;
            if let Some(s) = stats {
                s.record_compaction_bytes(result.file_size);
            }
            new_files.push((
                target_level as u32,
                FileMetaData {
                    number: current_file_number,
                    file_size: result.file_size,
                    smallest_key: result.smallest_key.unwrap_or_default(),
                    largest_key: result.largest_key.unwrap_or_default(),
                    has_range_deletions: result.has_range_deletions,
                },
            ));
        }
    }

    // Flush remaining builder
    if let Some(b) = builder {
        let result = b.finish().c(d!())?;
        if let Some(s) = stats {
            s.record_compaction_bytes(result.file_size);
        }
        new_files.push((
            target_level as u32,
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
        return Err(eg!("sub-compaction merge error: {}", e));
    }

    Ok(SubCompactionOutput {
        new_files,
        files_produced: next_file_idx,
    })
}

impl LeveledCompaction {
    /// Check if compaction is needed and return a task if so.
    pub fn pick_compaction(version: &Version, options: &DbOptions) -> Option<CompactionTask> {
        // Priority 1: L0 → L1 when L0 has too many files
        if version.l0_file_count() >= options.l0_compaction_trigger {
            return Self::pick_l0_compaction(version);
        }

        // Priority 2: Check each level for size overflow
        for level in 1..version.num_levels - 1 {
            let level_size: u64 = version
                .level_files(level)
                .iter()
                .map(|f| f.meta.file_size)
                .sum();
            let max_size = Self::max_bytes_for_level(options, level);
            if level_size > max_size {
                return Self::pick_level_compaction(version, level);
            }
        }

        None
    }

    /// Pick L0 → L1 compaction.
    fn pick_l0_compaction(version: &Version) -> Option<CompactionTask> {
        let l0_files = version.level_files(0);
        if l0_files.is_empty() {
            return None;
        }

        // All L0 files participate (they may overlap with each other)
        let input_l0: Vec<TableFile> = l0_files.to_vec();

        // Find the total key range of L0 files
        let (smallest, largest) = Self::total_key_range(&input_l0);

        // Find overlapping L1 files
        let input_l1 = Self::overlapping_files(version.level_files(1), &smallest, &largest);

        Some(CompactionTask {
            level: 0,
            input_files_level: input_l0,
            input_files_next: input_l1,
        })
    }

    /// Pick Ln → Ln+1 compaction.
    fn pick_level_compaction(version: &Version, level: usize) -> Option<CompactionTask> {
        let files = version.level_files(level);
        if files.is_empty() {
            return None;
        }

        // Pick the largest file in this level
        let target = files.iter().max_by_key(|f| f.meta.file_size)?;
        let input_level = vec![target.clone()];

        let (smallest, largest) = Self::total_key_range(&input_level);
        let next_level = level + 1;
        let input_next = if next_level < version.num_levels {
            Self::overlapping_files(version.level_files(next_level), &smallest, &largest)
        } else {
            Vec::new()
        };

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
            let file_smallest = crate::types::user_key(&tf.meta.smallest_key);
            let file_largest = crate::types::user_key(&tf.meta.largest_key);
            let overlaps_begin = begin.is_none_or(|b| file_largest >= b);
            let overlaps_end = end.is_none_or(|e| file_smallest < e);
            if overlaps_begin && overlaps_end {
                input_l0.push(tf.clone());
            }
        }

        if !input_l0.is_empty() {
            let (smallest, largest) = Self::total_key_range(&input_l0);
            let input_l1 = Self::overlapping_files(version.level_files(1), &smallest, &largest);
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
                let file_smallest = crate::types::user_key(&tf.meta.smallest_key);
                let file_largest = crate::types::user_key(&tf.meta.largest_key);
                let overlaps_begin = begin.is_none_or(|b| file_largest >= b);
                let overlaps_end = end.is_none_or(|e| file_smallest < e);
                if overlaps_begin && overlaps_end {
                    input_level.push(tf.clone());
                }
            }

            if !input_level.is_empty() {
                let (smallest, largest) = Self::total_key_range(&input_level);
                let next_level = level + 1;
                let input_next = if next_level < version.num_levels {
                    Self::overlapping_files(version.level_files(next_level), &smallest, &largest)
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

    /// Execute a compaction: stream-merge input files and produce new SST files.
    /// Memory usage: O(input_files × block_size) instead of O(total_data).
    pub fn execute_compaction(
        task: &CompactionTask,
        versions: &mut VersionSet,
        db_path: &Path,
        options: &DbOptions,
    ) -> Result<()> {
        Self::execute_compaction_with_cache(task, versions, db_path, options, None, None, None, &[])
    }

    /// Execute compaction with optional table cache for eviction and rate limiter.
    ///
    /// `active_snapshots` lists sequence numbers of all open snapshots.
    /// Keys with sequence numbers below the smallest active snapshot get
    /// their sequence zeroed (P5.1). At the bottommost level, tombstones
    /// are dropped (P5.2).
    #[allow(clippy::too_many_arguments)]
    /// Convenience wrapper: runs compaction I/O and installs the result.
    /// Requires `&mut VersionSet` for the entire duration — use
    /// `execute_compaction_io` + `install_compaction` separately when
    /// you want to release the lock during I/O.
    pub fn execute_compaction_with_cache(
        task: &CompactionTask,
        versions: &mut VersionSet,
        db_path: &Path,
        options: &DbOptions,
        table_cache: Option<&Arc<TableCache>>,
        rate_limiter: Option<&Arc<crate::rate_limiter::RateLimiter>>,
        stats: Option<&Arc<crate::stats::DbStats>>,
        active_snapshots: &[SequenceNumber],
    ) -> Result<()> {
        let target_level = task.level + 1;

        // Trivial move optimization: if there's exactly one input file and no
        // overlap with the next level, just move the metadata without rewriting.
        // Skip the optimisation when a compaction filter is installed, because
        // the filter needs to see every key-value pair to decide on removals.
        if task.input_files_level.len() == 1
            && task.input_files_next.is_empty()
            && options.compaction_filter.is_none()
        {
            let tf = &task.input_files_level[0];
            let mut edit = VersionEdit::new();
            edit.delete_file(task.level as u32, tf.meta.number);
            edit.add_file(target_level as u32, tf.meta.clone());
            edit.set_next_file_number(versions.next_file_number());
            versions.log_and_apply(edit).c(d!())?;
            if let Some(s) = stats {
                s.record_compaction_completed();
            }
            return Ok(());
        }

        let max_outputs = task.total_input_size() / options.target_file_size_base
            + options.max_subcompactions.max(1) as u64;
        let file_number_start = versions.reserve_file_numbers(max_outputs);

        let output = Self::execute_compaction_io(
            task,
            file_number_start,
            db_path,
            options,
            rate_limiter,
            stats,
            active_snapshots,
        )
        .c(d!())?;

        Self::install_compaction(output, versions, table_cache, db_path, stats).c(d!())
    }

    /// Pure I/O phase of compaction — does NOT require any lock.
    ///
    /// Merges input files, writes output SSTs using pre-allocated file numbers,
    /// and returns a `CompactionOutput` ready for `install_compaction`.
    ///
    /// When `options.max_subcompactions > 1` and the target level has enough
    /// files to split on, the work is divided into parallel sub-compactions
    /// using `std::thread::scope`.
    #[allow(clippy::too_many_arguments)]
    pub fn execute_compaction_io(
        task: &CompactionTask,
        file_number_start: u64,
        db_path: &Path,
        options: &DbOptions,
        rate_limiter: Option<&Arc<crate::rate_limiter::RateLimiter>>,
        stats: Option<&Arc<crate::stats::DbStats>>,
        active_snapshots: &[SequenceNumber],
    ) -> Result<CompactionOutput> {
        let target_level = task.level + 1;
        let is_bottommost = target_level >= options.num_levels - 1;
        let min_unflushed_seq = active_snapshots
            .iter()
            .min()
            .copied()
            .unwrap_or(SequenceNumber::MAX);

        let target_compression = if !options.compression_per_level.is_empty()
            && target_level < options.compression_per_level.len()
        {
            options.compression_per_level[target_level]
        } else {
            options.compression
        };
        let build_opts = TableBuildOptions {
            block_size: options.block_size,
            block_restart_interval: options.block_restart_interval,
            bloom_bits_per_key: options.bloom_bits_per_key,
            internal_keys: true,
            compression: target_compression,
            prefix_len: options.prefix_len,
            block_property_collectors: Vec::new(),
        };

        // Compute split points from target-level file boundaries.
        // L0 files overlap arbitrarily, so every sub-task would have to
        // include all L0 sources — redundant I/O with no benefit.
        // Sub-compaction is only useful for Ln→Ln+1 (level >= 1).
        let max_subs = if task.level == 0 {
            1
        } else {
            options.max_subcompactions.max(1)
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
        let all_range_del_entries = collect_range_del_entries(&all_input_files).c(d!())?;
        let all_raw_tombstones = collect_raw_tombstones(&all_input_files).c(d!())?;

        // Shared atomic counter for thread-safe file number allocation
        let file_counter = AtomicU64::new(file_number_start);

        let sub_outputs = if actual_subs <= 1 {
            // Fast path: single sub-compaction (zero overhead)
            vec![
                execute_sub_compaction_io(
                    &sub_tasks[0],
                    target_level,
                    is_bottommost,
                    db_path,
                    &build_opts,
                    options,
                    rate_limiter,
                    stats,
                    active_snapshots,
                    min_unflushed_seq,
                    &file_counter,
                    &all_range_del_entries,
                    &all_raw_tombstones,
                )
                .c(d!())?,
            ]
        } else {
            // Parallel sub-compactions
            let thread_results: Vec<Result<SubCompactionOutput>> = std::thread::scope(|s| {
                let handles: Vec<_> = sub_tasks
                    .iter()
                    .map(|sub| {
                        s.spawn(|| {
                            execute_sub_compaction_io(
                                sub,
                                target_level,
                                is_bottommost,
                                db_path,
                                &build_opts,
                                options,
                                rate_limiter,
                                stats,
                                active_snapshots,
                                min_unflushed_seq,
                                &file_counter,
                                &all_range_del_entries,
                                &all_raw_tombstones,
                            )
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| match h.join() {
                        Ok(r) => r,
                        Err(_) => Err(eg!("sub-compaction thread panicked")),
                    })
                    .collect()
            });
            let mut outputs = Vec::with_capacity(thread_results.len());
            for r in thread_results {
                outputs.push(r.c(d!())?);
            }
            outputs
        };

        // Merge sub-compaction outputs
        let mut edit = VersionEdit::new();
        let mut total_files_produced = 0u64;
        for sub_out in sub_outputs {
            for file_entry in sub_out.new_files {
                edit.new_files.push(file_entry);
            }
            total_files_produced += sub_out.files_produced;
        }

        // Record deletions (orchestrator responsibility)
        let input_file_numbers: HashSet<u64> = task
            .input_files_level
            .iter()
            .map(|f| f.meta.number)
            .chain(task.input_files_next.iter().map(|f| f.meta.number))
            .collect();

        for tf in &task.input_files_level {
            edit.delete_file(task.level as u32, tf.meta.number);
        }
        for tf in &task.input_files_next {
            edit.delete_file(target_level as u32, tf.meta.number);
        }

        Ok(CompactionOutput {
            edit,
            input_file_numbers,
            files_produced: total_files_produced,
            next_file_number_hint: file_counter.load(Ordering::Relaxed),
        })
    }

    /// Install the result of a compaction: apply the VersionEdit, evict
    /// old files from cache, and delete old SST files from disk.
    /// Requires `&mut VersionSet` (hold the lock for this short phase only).
    pub fn install_compaction(
        mut output: CompactionOutput,
        versions: &mut VersionSet,
        table_cache: Option<&Arc<TableCache>>,
        db_path: &Path,
        stats: Option<&Arc<crate::stats::DbStats>>,
    ) -> Result<()> {
        // Guard against stale compaction results: if any input file has already
        // been removed from the current version (e.g. by a concurrent inline
        // compaction), this output is based on outdated data and must be
        // discarded.  Delete orphaned output SSTs and return early.
        {
            let version = versions.current();
            let all_file_numbers: HashSet<u64> = (0..version.num_levels)
                .flat_map(|l| version.level_files(l).iter().map(|f| f.meta.number))
                .collect();
            let stale = output
                .input_file_numbers
                .iter()
                .any(|n| !all_file_numbers.contains(n));
            if stale {
                // Clean up output SST files that were written during the
                // (now-invalidated) I/O phase.
                for (_, meta) in &output.edit.new_files {
                    let orphan = db_path.join(format!("{:06}.sst", meta.number));
                    let _ = std::fs::remove_file(&orphan);
                }
                if let Some(cache) = table_cache {
                    for (_, meta) in &output.edit.new_files {
                        cache.evict(meta.number);
                    }
                }
                return Ok(());
            }
        }

        // Ensure next_file_number accounts for all numbers consumed by
        // sub-compaction threads (which use an atomic counter that may
        // exceed the pre-allocated range in edge cases).
        versions.ensure_file_number_at_least(output.next_file_number_hint);
        output
            .edit
            .set_next_file_number(versions.next_file_number());
        versions.log_and_apply(output.edit).c(d!())?;

        // Evict from table cache before deleting SST files
        if let Some(cache) = table_cache {
            for num in &output.input_file_numbers {
                cache.evict(*num);
            }
        }

        // Delete old SST files
        for num in &output.input_file_numbers {
            let old_path = db_path.join(format!("{:06}.sst", num));
            if let Err(e) = std::fs::remove_file(&old_path) {
                tracing::warn!("failed to remove old SST {}: {}", old_path.display(), e);
            }
        }

        if let Some(s) = stats {
            s.record_compaction_completed();
        }

        Ok(())
    }

    /// Force-merge all files at a given level into one output at the same level.
    /// Drops tombstones if this is the bottommost level.
    #[allow(clippy::too_many_arguments)]
    pub fn force_merge_level(
        level: usize,
        versions: &mut VersionSet,
        db_path: &Path,
        options: &DbOptions,
        table_cache: Option<&Arc<TableCache>>,
        rate_limiter: Option<&Arc<crate::rate_limiter::RateLimiter>>,
        stats: Option<&Arc<crate::stats::DbStats>>,
        active_snapshots: &[SequenceNumber],
    ) -> Result<()> {
        let is_bottommost = level >= options.num_levels - 1;
        let min_unflushed_seq = active_snapshots
            .iter()
            .min()
            .copied()
            .unwrap_or(SequenceNumber::MAX);
        let version = versions.current();
        let files = version.level_files(level);
        if files.len() <= 1 {
            return Ok(());
        }

        let mut sources: Vec<IterSource> = Vec::new();
        for tf in files {
            let iter = TableIterator::new(tf.reader.clone());
            sources.push(IterSource::from_boxed(Box::new(iter)));
        }

        // Inject range tombstones from new-format SSTs into the merge stream
        let range_del_entries = collect_range_del_entries(files).c(d!())?;
        if !range_del_entries.is_empty() {
            sources.push(IterSource::new(range_del_entries));
        }

        let mut merger = MergingIterator::new(sources, compare_internal_key);

        let compression = if !options.compression_per_level.is_empty()
            && level < options.compression_per_level.len()
        {
            options.compression_per_level[level]
        } else {
            options.compression
        };
        // build_opts is a template; block_property_collectors are created fresh
        // per output file (via factory functions) to avoid sharing mutable state.
        let build_opts = TableBuildOptions {
            block_size: options.block_size,
            block_restart_interval: options.block_restart_interval,
            bloom_bits_per_key: options.bloom_bits_per_key,
            internal_keys: true,
            compression,
            prefix_len: options.prefix_len,
            block_property_collectors: Vec::new(),
        };

        let mut edit = VersionEdit::new();
        let mut builder: Option<TableBuilder> = None;
        let mut current_file_number = 0u64;
        let mut current_size = 0usize;
        let mut last_point_key: Option<Vec<u8>> = None;
        let mut last_range_del_key: Option<Vec<u8>> = None;
        let mut last_written_seq: SequenceNumber = 0;
        let mut snapshot_idx: usize = active_snapshots.len();
        let mut range_tombstones = RangeTombstoneTracker::new();

        while let Some((ikey, value)) = merger.next_entry() {
            if ikey.len() < 8 {
                continue;
            }
            let ikr = InternalKeyRef::new(&ikey);
            let user_key = ikr.user_key();

            if ikr.value_type() == ValueType::RangeDeletion {
                range_tombstones.add(user_key.to_vec(), value.as_slice().to_vec(), ikr.sequence());
                range_tombstones.reset();
                if let Some(ref last) = last_range_del_key
                    && last.as_slice() == user_key
                {
                    continue;
                }
                last_range_del_key = Some(user_key.to_vec());
                if is_bottommost {
                    continue;
                }
            } else if let Some(ref last) = last_point_key
                && last.as_slice() == user_key
            {
                // Same key — check if a snapshot needs this version.
                while snapshot_idx > 0 && active_snapshots[snapshot_idx - 1] >= last_written_seq {
                    snapshot_idx -= 1;
                }
                if snapshot_idx > 0 && active_snapshots[snapshot_idx - 1] >= ikr.sequence() {
                    last_written_seq = ikr.sequence();
                } else {
                    continue;
                }
            } else {
                last_point_key = Some(user_key.to_vec());
                last_written_seq = ikr.sequence();
                snapshot_idx = active_snapshots.len();

                if ikr.value_type() == ValueType::Deletion && is_bottommost {
                    continue;
                }

                if ikr.value_type() == ValueType::Value && !range_tombstones.is_empty() {
                    let entry_seq = ikr.sequence();
                    if range_tombstones.is_deleted(user_key, entry_seq, SequenceNumber::MAX) {
                        continue;
                    }
                }
            }

            // Apply compaction filter
            if let Some(ref filter) = options.compaction_filter
                && ikr.value_type() == ValueType::Value
            {
                use crate::options::CompactionFilterDecision;
                match filter.filter(level, user_key, value.as_slice()) {
                    CompactionFilterDecision::Keep => {}
                    CompactionFilterDecision::Remove => continue,
                    CompactionFilterDecision::ChangeValue(_) => {
                        // force_merge_level doesn't rewrite values — the output
                        // SST reuses the existing encoded block. Silently treat
                        // as Keep. Regular level-to-level compaction handles
                        // ChangeValue correctly.
                        debug_assert!(
                            false,
                            "CompactionFilter returned ChangeValue in force_merge_level, \
                             which cannot rewrite values — treating as Keep"
                        );
                    }
                }
            }

            if builder.is_none() {
                current_file_number = versions.new_file_number();
                let sst_path = db_path.join(format!("{:06}.sst", current_file_number));
                let mut opts = build_opts.clone();
                opts.block_property_collectors = options
                    .block_property_collectors
                    .iter()
                    .map(|f| f())
                    .collect();
                builder = Some(TableBuilder::new(&sst_path, opts).c(d!())?);
                current_size = 0;
            }

            // Sequence zeroing: at the bottommost level, if the entry's
            // sequence falls below the minimum active snapshot, zero it out.
            let final_ikey;
            let ikey_ref = if is_bottommost
                && ikr.sequence() > 0
                && ikr.sequence() < min_unflushed_seq
                && ikr.value_type() == ValueType::Value
            {
                final_ikey = InternalKey::new(user_key, 0, ikr.value_type())
                    .as_bytes()
                    .to_vec();
                &final_ikey
            } else {
                &ikey
            };

            builder
                .as_mut()
                .unwrap()
                .add(ikey_ref, value.as_slice())
                .c(d!())?;
            let entry_bytes = ikey_ref.len() + value.len();
            current_size += entry_bytes;

            // Rate-limit compaction writes
            if let Some(rl) = rate_limiter {
                rl.request(entry_bytes);
            }

            if current_size >= options.target_file_size_base as usize {
                let result = builder.take().unwrap().finish().c(d!())?;
                if let Some(s) = stats {
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
        }

        if let Some(b) = builder {
            let result = b.finish().c(d!())?;
            if let Some(s) = stats {
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
            return Err(eg!("force_merge iterator error: {}", e));
        }

        let input_file_numbers: HashSet<u64> = files.iter().map(|f| f.meta.number).collect();
        for tf in files {
            edit.delete_file(level as u32, tf.meta.number);
        }

        edit.set_next_file_number(versions.next_file_number());
        versions.log_and_apply(edit).c(d!())?;

        if let Some(cache) = table_cache {
            for num in &input_file_numbers {
                cache.evict(*num);
            }
        }

        for num in &input_file_numbers {
            let old_path = db_path.join(format!("{:06}.sst", num));
            if let Err(e) = std::fs::remove_file(&old_path) {
                tracing::warn!("failed to remove old SST {}: {}", old_path.display(), e);
            }
        }

        if let Some(s) = stats {
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
        Self::pick_level_compaction(version, level)
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
                || compare_internal_key(&f.meta.smallest_key, &smallest) == std::cmp::Ordering::Less
            {
                smallest = f.meta.smallest_key.clone();
            }
            if largest.is_empty()
                || compare_internal_key(&f.meta.largest_key, &largest)
                    == std::cmp::Ordering::Greater
            {
                largest = f.meta.largest_key.clone();
            }
        }

        (smallest, largest)
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
        let smallest_uk = crate::types::user_key(smallest);
        let largest_uk = crate::types::user_key(largest);
        files
            .iter()
            .filter(|f| {
                let file_largest_uk = crate::types::user_key(&f.meta.largest_key);
                let file_smallest_uk = crate::types::user_key(&f.meta.smallest_key);
                // File overlaps if: file.largest_uk >= smallest_uk AND file.smallest_uk <= largest_uk
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
}
