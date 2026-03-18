//! Leveled compaction strategy.
//!
//! L0 → L1: merge all overlapping L0 files with overlapping L1 files.
//! Ln → Ln+1: pick a file from Ln, merge with overlapping files in Ln+1.
//!
//! After compaction, the old files are deleted and new files are installed.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use crate::cache::table_cache::TableCache;
use crate::error::Result;
use crate::iterator::merge::{IterSource, MergingIterator};
use crate::manifest::version::{TableFile, Version};
use crate::manifest::version_edit::{FileMetaData, VersionEdit};
use crate::manifest::version_set::VersionSet;
use crate::options::DbOptions;
use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
use crate::sst::table_reader::TableIterator;
use crate::types::{InternalKeyRef, SequenceNumber, ValueType, compare_internal_key};

/// Description of a compaction to perform.
pub struct CompactionTask {
    /// Source level (files to compact from).
    pub level: usize,
    /// Files from the source level.
    pub input_files_level: Vec<TableFile>,
    /// Files from the target level (level + 1).
    pub input_files_next: Vec<TableFile>,
}

pub struct LeveledCompaction;

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
        Self::execute_compaction_with_cache(task, versions, db_path, options, None, None, None)
    }

    /// Execute compaction with optional table cache for eviction and rate limiter.
    pub fn execute_compaction_with_cache(
        task: &CompactionTask,
        versions: &mut VersionSet,
        db_path: &Path,
        options: &DbOptions,
        table_cache: Option<&Arc<TableCache>>,
        rate_limiter: Option<&Arc<crate::rate_limiter::RateLimiter>>,
        stats: Option<&Arc<crate::stats::DbStats>>,
    ) -> Result<()> {
        let target_level = task.level + 1;

        // Trivial move optimization: if there's exactly one input file and no
        // overlap with the next level, just move the metadata without rewriting.
        if task.input_files_level.len() == 1 && task.input_files_next.is_empty() {
            let tf = &task.input_files_level[0];
            let mut edit = VersionEdit::new();
            edit.delete_file(task.level as u32, tf.meta.number);
            edit.add_file(target_level as u32, tf.meta.clone());
            edit.set_next_file_number(versions.next_file_number());
            versions.log_and_apply(edit)?;
            if let Some(s) = stats {
                s.record_compaction_completed();
            }
            return Ok(());
        }

        // Build streaming merge sources from input files
        let mut sources: Vec<IterSource> = Vec::new();

        for tf in &task.input_files_level {
            let iter = TableIterator::new(tf.reader.clone());
            sources.push(IterSource::from_boxed(Box::new(iter)));
        }
        for tf in &task.input_files_next {
            let iter = TableIterator::new(tf.reader.clone());
            sources.push(IterSource::from_boxed(Box::new(iter)));
        }

        // Streaming merge in internal key order (lex = logical order)
        let mut merger = MergingIterator::new(sources, compare_internal_key);

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
        };

        let mut edit = VersionEdit::new();
        let mut builder: Option<TableBuilder> = None;
        let mut current_file_number = 0u64;
        let mut current_size = 0usize;
        let mut last_user_key: Option<Vec<u8>> = None;
        // Collect range tombstones with sequence numbers to filter covered keys.
        // A range tombstone only deletes entries with seq < tombstone_seq.
        let mut range_tombstones: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> = Vec::new();

        while let Some((ikey, value)) = merger.next_entry() {
            if ikey.len() < 8 {
                continue;
            }
            let ikr = InternalKeyRef::new(&ikey);
            let user_key = ikr.user_key();

            // Collect range tombstones for filtering
            if ikr.value_type() == ValueType::RangeDeletion {
                range_tombstones.push((user_key.to_vec(), value.clone(), ikr.sequence()));
                // Skip older versions of the begin key
                if let Some(ref last) = last_user_key
                    && last.as_slice() == user_key
                {
                    continue;
                }
                last_user_key = Some(user_key.to_vec());
                // Drop range tombstones at the bottommost level
                if target_level >= options.num_levels - 1 {
                    continue;
                }
                // Keep range tombstone in non-bottommost levels — fall through to write it
            } else {
                // Deduplicate: skip older versions of same user key
                if let Some(ref last) = last_user_key
                    && last.as_slice() == user_key
                {
                    continue;
                }
                last_user_key = Some(user_key.to_vec());

                // Drop point tombstones at the bottommost level
                if ikr.value_type() == ValueType::Deletion && target_level >= options.num_levels - 1
                {
                    continue;
                }

                // Skip keys covered by range tombstones
                if ikr.value_type() == ValueType::Value {
                    let entry_seq = ikr.sequence();
                    let covered = range_tombstones.iter().any(|(begin, end, tomb_seq)| {
                        user_key >= begin.as_slice()
                            && user_key < end.as_slice()
                            && *tomb_seq > entry_seq
                    });
                    if covered {
                        continue;
                    }
                }
            }

            // Apply compaction filter (if configured)
            let mut final_value = value;
            if let Some(ref filter) = options.compaction_filter
                && ikr.value_type() == ValueType::Value
            {
                use crate::options::CompactionFilterDecision;
                match filter.filter(target_level, user_key, &final_value) {
                    CompactionFilterDecision::Keep => {}
                    CompactionFilterDecision::Remove => continue,
                    CompactionFilterDecision::ChangeValue(new_val) => {
                        final_value = new_val;
                    }
                }
            }

            // Create new output file if needed
            if builder.is_none() {
                current_file_number = versions.new_file_number();
                let sst_path = db_path.join(format!("{:06}.sst", current_file_number));
                builder = Some(TableBuilder::new(&sst_path, build_opts.clone())?);
                current_size = 0;
            }

            let entry_bytes = ikey.len() + final_value.len();
            builder.as_mut().unwrap().add(&ikey, &final_value)?;
            current_size += entry_bytes;

            // Rate-limit compaction writes
            if let Some(rl) = rate_limiter {
                rl.request(entry_bytes);
            }

            // Split output file if target size reached
            if current_size >= options.target_file_size_base as usize {
                let result = builder.take().unwrap().finish()?;
                if let Some(s) = stats {
                    s.record_compaction_bytes(result.file_size);
                }
                edit.add_file(
                    target_level as u32,
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

        // Flush remaining builder
        if let Some(b) = builder {
            let result = b.finish()?;
            if let Some(s) = stats {
                s.record_compaction_bytes(result.file_size);
            }
            edit.add_file(
                target_level as u32,
                FileMetaData {
                    number: current_file_number,
                    file_size: result.file_size,
                    smallest_key: result.smallest_key.unwrap_or_default(),
                    largest_key: result.largest_key.unwrap_or_default(),
                    has_range_deletions: result.has_range_deletions,
                },
            );
        }

        // Record deletions
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

        edit.set_next_file_number(versions.next_file_number());
        versions.log_and_apply(edit)?;

        // Evict from table cache before deleting SST files
        if let Some(cache) = table_cache {
            for num in &input_file_numbers {
                cache.evict(*num);
            }
        }

        // Delete old SST files
        for num in &input_file_numbers {
            let old_path = db_path.join(format!("{:06}.sst", num));
            let _ = std::fs::remove_file(old_path);
        }

        if let Some(s) = stats {
            s.record_compaction_completed();
        }

        Ok(())
    }

    /// Force-merge all files at a given level into one output at the same level.
    /// Drops tombstones if this is the bottommost level.
    pub fn force_merge_level(
        level: usize,
        versions: &mut VersionSet,
        db_path: &Path,
        options: &DbOptions,
        table_cache: Option<&Arc<TableCache>>,
        rate_limiter: Option<&Arc<crate::rate_limiter::RateLimiter>>,
        stats: Option<&Arc<crate::stats::DbStats>>,
    ) -> Result<()> {
        let is_bottommost = level >= options.num_levels - 1;
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

        let mut merger = MergingIterator::new(sources, compare_internal_key);

        let compression = if !options.compression_per_level.is_empty()
            && level < options.compression_per_level.len()
        {
            options.compression_per_level[level]
        } else {
            options.compression
        };
        let build_opts = TableBuildOptions {
            block_size: options.block_size,
            block_restart_interval: options.block_restart_interval,
            bloom_bits_per_key: options.bloom_bits_per_key,
            internal_keys: true,
            compression,
            prefix_len: options.prefix_len,
        };

        let mut edit = VersionEdit::new();
        let mut builder: Option<TableBuilder> = None;
        let mut current_file_number = 0u64;
        let mut current_size = 0usize;
        let mut last_user_key: Option<Vec<u8>> = None;
        let mut range_tombstones: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> = Vec::new();

        while let Some((ikey, value)) = merger.next_entry() {
            if ikey.len() < 8 {
                continue;
            }
            let ikr = InternalKeyRef::new(&ikey);
            let user_key = ikr.user_key();

            if ikr.value_type() == ValueType::RangeDeletion {
                range_tombstones.push((user_key.to_vec(), value.clone(), ikr.sequence()));
                if let Some(ref last) = last_user_key
                    && last.as_slice() == user_key
                {
                    continue;
                }
                last_user_key = Some(user_key.to_vec());
                if is_bottommost {
                    continue;
                }
            } else {
                if let Some(ref last) = last_user_key
                    && last.as_slice() == user_key
                {
                    continue;
                }
                last_user_key = Some(user_key.to_vec());

                if ikr.value_type() == ValueType::Deletion && is_bottommost {
                    continue;
                }

                if ikr.value_type() == ValueType::Value {
                    let entry_seq = ikr.sequence();
                    let covered = range_tombstones.iter().any(|(begin, end, tomb_seq)| {
                        user_key >= begin.as_slice()
                            && user_key < end.as_slice()
                            && *tomb_seq > entry_seq
                    });
                    if covered {
                        continue;
                    }
                }
            }

            if builder.is_none() {
                current_file_number = versions.new_file_number();
                let sst_path = db_path.join(format!("{:06}.sst", current_file_number));
                builder = Some(TableBuilder::new(&sst_path, build_opts.clone())?);
                current_size = 0;
            }

            builder.as_mut().unwrap().add(&ikey, &value)?;
            current_size += ikey.len() + value.len();

            // Rate-limit compaction writes
            if let Some(rl) = rate_limiter {
                rl.request(ikey.len() + value.len());
            }

            if current_size >= options.target_file_size_base as usize {
                let result = builder.take().unwrap().finish()?;
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
            let result = b.finish()?;
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

        let input_file_numbers: HashSet<u64> = files.iter().map(|f| f.meta.number).collect();
        for tf in files {
            edit.delete_file(level as u32, tf.meta.number);
        }

        edit.set_next_file_number(versions.next_file_number());
        versions.log_and_apply(edit)?;

        if let Some(cache) = table_cache {
            for num in &input_file_numbers {
                cache.evict(*num);
            }
        }

        for num in &input_file_numbers {
            let old_path = db_path.join(format!("{:06}.sst", num));
            let _ = std::fs::remove_file(old_path);
        }

        if let Some(s) = stats {
            s.record_compaction_completed();
        }

        Ok(())
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
    /// Uses `compare_internal_key` for correct variable-length user key ordering.
    fn overlapping_files(files: &[TableFile], smallest: &[u8], largest: &[u8]) -> Vec<TableFile> {
        files
            .iter()
            .filter(|f| {
                // File overlaps if: file.largest >= smallest AND file.smallest <= largest
                compare_internal_key(&f.meta.largest_key, smallest) != std::cmp::Ordering::Less
                    && compare_internal_key(&f.meta.smallest_key, largest)
                        != std::cmp::Ordering::Greater
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
}
