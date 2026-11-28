//! LevelIterator: a lazy two-level iterator over non-overlapping SST files at a single level.
//!
//! Instead of opening all SST files upfront, it binary-searches the file list on seek
//! and opens one file's TableIterator at a time. This reduces MergingIterator heap size
//! from O(total_files) to O(L0_count + num_levels).

use std::{cmp::Ordering, ops::Deref, sync::Arc};

use crate::Error;
use crate::iterator::merge::SeekableIterator;
use crate::manifest::version::{TableFile, Version};
use crate::options::IterConfig;
use crate::sst::table_reader::TableIterator;
use crate::types::{LazyValue, compare_internal_key, user_key as user_key_from_internal};

/// A lazy iterator over a sorted, non-overlapping set of SST files (L1+).
///
/// Opens one file's `TableIterator` at a time, advancing to the next file
/// only when the current one is exhausted.
/// The files of one level, read in place from a pinned `Version` so creating
/// a DB iterator copies no file metadata. Tests may supply an owned list.
enum LevelFiles {
    Version {
        version: Arc<Version>,
        level: usize,
    },
    #[cfg(test)]
    Owned(Vec<TableFile>),
}

impl Deref for LevelFiles {
    type Target = [TableFile];

    fn deref(&self) -> &[TableFile] {
        match self {
            Self::Version { version, level } => version.level_files(*level),
            #[cfg(test)]
            Self::Owned(files) => files,
        }
    }
}

/// Result of checking one file against a `LevelIterator`'s filters.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FileFilter {
    Pass,
    /// Rejected by the prefix bloom; neighbouring files may still match.
    Skip,
    /// Entirely below the start hint, as is every earlier file.
    BeforeRange,
    /// Entirely at or past the end hint, as is every later file.
    AfterRange,
}

#[cfg(test)]
thread_local! {
    /// Files checked against the range and prefix filters on this thread.
    static FILES_FILTERED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub struct LevelIterator {
    /// L1+ files, sorted by smallest_key (non-overlapping).
    files: LevelFiles,
    /// Current position in `files`. files.len() = exhausted.
    file_index: usize,
    /// Lazily opened iterator for the file at file_index.
    current_iter: Option<TableIterator>,
    config: Arc<IterConfig>,
    /// First I/O error encountered while iterating or repositioning. Once
    /// set, iteration and subsequent seeks stay halted instead of silently
    /// clearing the failure or advancing past the errored file.
    error: Option<Error>,
    /// Whether data-block cache misses populate the block cache (default: true).
    fill_cache: bool,
    /// Whether index-leaf cache misses populate the block cache (default: true).
    fill_index_cache: bool,
}

impl LevelIterator {
    /// Create a new LevelIterator over non-overlapping files sorted by key range.
    #[cfg(test)]
    pub fn new(files: Vec<TableFile>) -> Self {
        Self::with_files(LevelFiles::Owned(files))
    }

    /// Iterate `level` of a pinned `version` without copying its file list.
    pub fn for_level(version: Arc<Version>, level: usize) -> Self {
        Self::with_files(LevelFiles::Version { version, level })
    }

    fn with_files(files: LevelFiles) -> Self {
        Self {
            files,
            file_index: 0,
            current_iter: None,
            config: IterConfig::empty(),
            error: None,
            fill_cache: true,
            fill_index_cache: true,
        }
    }

    /// Set whether cache misses populate the block cache (default: true).
    /// Forwarded to every `TableIterator` this level iterator opens.
    pub fn with_fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
        self.fill_index_cache = fill_cache;
        self
    }

    /// Set whether index-leaf cache misses populate the block cache.
    /// Forwarded to every `TableIterator` this level iterator opens.
    pub fn with_fill_index_cache(mut self, fill_index_cache: bool) -> Self {
        self.fill_index_cache = fill_index_cache;
        self
    }

    /// Enable prefix bloom filter pruning.
    #[cfg(test)]
    pub fn with_prefix(mut self, prefix: Vec<u8>) -> Self {
        Arc::make_mut(&mut self.config).prefix = Some(prefix.into());
        self
    }

    /// Enable range-based file skipping.
    #[cfg(test)]
    pub fn with_range_hints(mut self, start: Option<Vec<u8>>, end: Option<Vec<u8>>) -> Self {
        self.config = Arc::new(IterConfig {
            lower: start,
            upper: end,
            ..Default::default()
        });
        self
    }

    fn has_empty_bounds(&self) -> bool {
        self.config
            .lower
            .as_ref()
            .zip(self.config.upper.as_ref())
            .is_some_and(|(a, b)| a >= b)
    }

    /// Check a file against all configured filters. Files in a level are
    /// sorted and disjoint, so a range rejection also rejects every file
    /// beyond it in that direction; only a prefix-bloom rejection is local.
    fn check_file_filters(&self, tf: &TableFile) -> FileFilter {
        #[cfg(test)]
        FILES_FILTERED.with(|n| n.set(n.get() + 1));
        if self.has_empty_bounds() {
            return FileFilter::AfterRange;
        }
        // Range filter: file's largest user key must be >= start_hint
        if let Some(ref start) = self.config.lower {
            let largest_uk = user_key_from_internal(&tf.meta.largest_key);
            if largest_uk < start.as_slice() {
                return FileFilter::BeforeRange;
            }
        }
        // Range filter: file's smallest user key must be < end_hint and < upper_bound
        let smallest_uk = user_key_from_internal(&tf.meta.smallest_key);
        if self
            .config
            .upper
            .as_deref()
            .is_some_and(|ub| smallest_uk >= ub)
        {
            return FileFilter::AfterRange;
        }
        // Prefix bloom filter
        if let Some(ref prefix) = self.config.prefix
            && !tf.reader.prefix_may_match(prefix)
        {
            return FileFilter::Skip;
        }
        FileFilter::Pass
    }

    fn latch_current_error_and_check(&mut self) -> bool {
        if self.error.is_none()
            && let Some(e) = self.current_iter.as_ref().and_then(|it| it.iter_error())
        {
            self.error = Some(e.clone());
        }
        if self.error.is_some() {
            self.current_iter = None;
            self.file_index = self.files.len();
            true
        } else {
            false
        }
    }

    /// Seek to the first entry >= target.
    fn seek_impl(&mut self, target: &[u8]) {
        if self.latch_current_error_and_check() {
            return;
        }
        // Binary search: find first file whose largest_key >= target.
        // partition_point returns first index where predicate is false.
        let idx = self.files.partition_point(|tf| {
            compare_internal_key(&tf.meta.largest_key, target) == Ordering::Less
        });
        if idx == self.file_index
            && self.current_iter.is_some()
            && idx < self.files.len()
            && matches!(self.check_file_filters(&self.files[idx]), FileFilter::Pass)
        {
            let iter = self.current_iter.as_mut().unwrap();
            iter.seek(target);
            if let Some(e) = iter.iter_error() {
                self.error = Some(e.clone());
                self.file_index = self.files.len();
                self.current_iter = None;
            }
            return;
        }
        self.file_index = idx;
        self.current_iter = None;
        self.open_file_and_seek(Some(target));
    }

    /// Open the file at file_index (skipping filtered files) and optionally seek.
    fn open_file_and_seek(&mut self, target: Option<&[u8]>) {
        while self.file_index < self.files.len() {
            let tf = &self.files[self.file_index];
            match self.check_file_filters(tf) {
                FileFilter::Pass => {}
                FileFilter::AfterRange => {
                    self.file_index = self.files.len();
                    self.current_iter = None;
                    return;
                }
                FileFilter::Skip | FileFilter::BeforeRange => {
                    self.file_index += 1;
                    continue;
                }
            }
            let mut table_iter = TableIterator::new(tf.reader.clone())
                .with_fill_cache(self.fill_cache)
                .with_fill_index_cache(self.fill_index_cache);
            table_iter.set_config(self.config.clone());
            if let Some(t) = target {
                table_iter.seek(t);
                if let Some(e) = table_iter.iter_error() {
                    // Repositioning failed with I/O error — stop instead of
                    // silently skipping this file's keys.
                    self.error = Some(e.clone());
                    self.file_index = self.files.len();
                    self.current_iter = None;
                    return;
                }
            }
            self.current_iter = Some(table_iter);
            return;
        }
        self.current_iter = None;
    }
}

impl Iterator for LevelIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() {
            return None;
        }
        loop {
            // If no iterator is open yet, try to open the current file.
            if self.current_iter.is_none() && self.file_index < self.files.len() {
                self.open_file_and_seek(None);
                self.current_iter.as_ref()?;
            }
            if let Some(ref mut iter) = self.current_iter {
                if let Some(entry) = iter.next() {
                    return Some(entry);
                }
                // Exhaustion may actually be a mid-file I/O error. Advancing
                // to the next file would silently truncate this file's
                // contribution — stop and surface the error instead.
                if let Some(e) = iter.iter_error() {
                    self.error = Some(e.clone());
                    self.current_iter = None;
                    return None;
                }
            }
            // Current file exhausted — move to next
            self.current_iter = None;
            self.file_index += 1;
            if self.file_index >= self.files.len() {
                return None;
            }
        }
    }
}

impl super::merge::SeekableIterator for LevelIterator {
    fn seek_to(&mut self, target: &[u8]) {
        self.seek_impl(target);
    }

    fn current(&self) -> Option<(Vec<u8>, LazyValue)> {
        let mut key_buf = Vec::new();
        let mut val_out = LazyValue::empty();
        self.current_into_lazy(&mut key_buf, &mut val_out)
            .then_some((key_buf, val_out))
    }

    fn current_into_lazy(&self, key_buf: &mut Vec<u8>, val_out: &mut LazyValue) -> bool {
        self.current_iter
            .as_ref()
            .is_some_and(|iter| iter.current_into_lazy(key_buf, val_out))
    }

    fn prev(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        let mut key_buf = Vec::new();
        let mut val_out = LazyValue::empty();
        self.prev_into_lazy(&mut key_buf, &mut val_out)
            .then_some((key_buf, val_out))
    }

    fn prev_into_lazy(&mut self, key_buf: &mut Vec<u8>, val_out: &mut LazyValue) -> bool {
        if self.error.is_some() {
            return false;
        }
        // Try prev within current table iterator
        if let Some(ref mut iter) = self.current_iter {
            if iter.prev_into_lazy(key_buf, val_out) {
                return true;
            }
            if let Some(e) = iter.iter_error() {
                self.error = Some(e.clone());
                self.current_iter = None;
                return false;
            }
        }
        // Current table exhausted backward — move to previous file
        loop {
            if self.file_index == 0 {
                self.current_iter = None;
                return false;
            }
            self.file_index -= 1;
            let tf = &self.files[self.file_index];
            match self.check_file_filters(tf) {
                FileFilter::Pass => {}
                FileFilter::BeforeRange => {
                    self.file_index = 0;
                    self.current_iter = None;
                    return false;
                }
                FileFilter::Skip | FileFilter::AfterRange => continue,
            }
            let mut table_iter = TableIterator::new(tf.reader.clone())
                .with_fill_cache(self.fill_cache)
                .with_fill_index_cache(self.fill_index_cache);
            table_iter.set_config(self.config.clone());
            table_iter.seek_to_last();
            if let Some(e) = table_iter.iter_error() {
                self.error = Some(e.clone());
                self.current_iter = None;
                return false;
            }
            if table_iter.current_into_lazy(key_buf, val_out) {
                self.current_iter = Some(table_iter);
                return true;
            }
        }
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        if self.latch_current_error_and_check() {
            self.file_index = 0;
            return;
        }
        if self.has_empty_bounds()
            || self
                .config
                .lower
                .as_deref()
                .is_some_and(|lb| user_key_from_internal(target) < lb)
        {
            self.file_index = 0;
            self.current_iter = None;
            return;
        }
        // Binary search: find last file whose smallest_key <= target.
        let idx = self.files.partition_point(|tf| {
            compare_internal_key(&tf.meta.smallest_key, target) != Ordering::Greater
        });
        if idx == 0 {
            self.file_index = 0;
            self.current_iter = None;
            return;
        }
        // Scan backward through candidate files. For non-overlapping files
        // only 1-2 tries are needed, but when prefix bloom filtering rejects
        // candidates we must continue scanning to avoid missing entries.
        // Early return on first match keeps this O(1) amortized.
        let start = idx - 1;
        let min_try = 0;
        for try_idx in (min_try..=start).rev() {
            let tf = &self.files[try_idx];
            match self.check_file_filters(tf) {
                FileFilter::Pass => {}
                FileFilter::BeforeRange => break,
                FileFilter::Skip | FileFilter::AfterRange => continue,
            }
            let mut table_iter = TableIterator::new(tf.reader.clone())
                .with_fill_cache(self.fill_cache)
                .with_fill_index_cache(self.fill_index_cache);
            table_iter.set_config(self.config.clone());
            table_iter.seek_for_prev(target);
            if let Some(e) = table_iter.iter_error() {
                // A repositioning I/O error is indistinguishable from "no
                // entry" via current(); skipping the file would silently
                // drop its keys from the scan. Stop and surface the error.
                self.error = Some(e.clone());
                self.file_index = 0;
                self.current_iter = None;
                return;
            }
            if table_iter.has_current() {
                self.file_index = try_idx;
                self.current_iter = Some(table_iter);
                return;
            }
        }
        self.file_index = 0;
        self.current_iter = None;
    }

    fn seek_to_first(&mut self) {
        if self.latch_current_error_and_check() {
            return;
        }
        self.file_index = 0;
        self.current_iter = None;
        self.open_file_and_seek(None);
    }

    fn seek_to_last(&mut self) {
        if self.latch_current_error_and_check() {
            self.file_index = 0;
            return;
        }
        self.current_iter = None;
        if self.has_empty_bounds() {
            self.file_index = 0;
            self.current_iter = None;
            return;
        }
        // Start from the last file and work backward
        for idx in (0..self.files.len()).rev() {
            let tf = &self.files[idx];
            match self.check_file_filters(tf) {
                FileFilter::Pass => {}
                FileFilter::BeforeRange => break,
                FileFilter::Skip | FileFilter::AfterRange => continue,
            }
            let mut table_iter = TableIterator::new(tf.reader.clone())
                .with_fill_cache(self.fill_cache)
                .with_fill_index_cache(self.fill_index_cache);
            table_iter.set_config(self.config.clone());
            table_iter.seek_to_last();
            if let Some(e) = table_iter.iter_error() {
                self.error = Some(e.clone());
                self.current_iter = None;
                return;
            }
            // The file passed the file-level filters, but block-property filters or
            // the upper bound may leave the table iterator unpositioned. Only accept
            // it if it actually yielded an entry; otherwise an earlier file holding
            // matching keys would be skipped (mirrors seek_for_prev above).
            if table_iter.has_current() {
                self.file_index = idx;
                self.current_iter = Some(table_iter);
                return;
            }
        }
    }

    fn next_into(&mut self, key_buf: &mut Vec<u8>, value_buf: &mut Vec<u8>) -> bool {
        if self.error.is_some() {
            return false;
        }
        loop {
            if self.current_iter.is_none() && self.file_index < self.files.len() {
                self.open_file_and_seek(None);
                if self.current_iter.is_none() {
                    return false;
                }
            }
            if let Some(ref mut iter) = self.current_iter {
                if iter.next_into(key_buf, value_buf) {
                    return true;
                }
                if let Some(e) = iter.iter_error() {
                    self.error = Some(e.clone());
                    self.current_iter = None;
                    return false;
                }
            }
            self.current_iter = None;
            self.file_index += 1;
            if self.file_index >= self.files.len() {
                return false;
            }
        }
    }

    fn prefetch_first_block(&mut self) {
        // Lazily open the file at file_index (skipping filtered files) if
        // none is open yet, mirroring the lazy-open pattern used by
        // next()/next_into(). Without this, a cold-start LevelIterator has
        // no open TableIterator to delegate the prefetch hint to, making
        // the hint a no-op for every L1+ level.
        if self.current_iter.is_none() && self.error.is_none() && self.file_index < self.files.len()
        {
            self.open_file_and_seek(None);
        }
        if let Some(ref mut iter) = self.current_iter {
            iter.prefetch_first_block();
        }
    }

    fn set_config(&mut self, config: Arc<IterConfig>) {
        self.config = config;
        if let Some(ref mut iter) = self.current_iter {
            iter.set_config(self.config.clone());
        }
    }

    fn iter_error(&self) -> Option<&Error> {
        self.error
            .as_ref()
            .or_else(|| self.current_iter.as_ref().and_then(|it| it.iter_error()))
    }

    fn advance_key(&mut self, key_buf: &mut Vec<u8>) -> bool {
        if self.error.is_some() {
            return false;
        }
        loop {
            if self.current_iter.is_none() && self.file_index < self.files.len() {
                self.open_file_and_seek(None);
                if self.current_iter.is_none() {
                    return false;
                }
            }
            if let Some(ref mut iter) = self.current_iter {
                if iter.advance_key(key_buf) {
                    return true;
                }
                if let Some(e) = iter.iter_error() {
                    self.error = Some(e.clone());
                    self.current_iter = None;
                    return false;
                }
            }
            self.current_iter = None;
            self.file_index += 1;
            if self.file_index >= self.files.len() {
                return false;
            }
        }
    }

    fn materialize_value_into(&self, val_out: &mut LazyValue) {
        if let Some(ref iter) = self.current_iter {
            iter.materialize_value_into(val_out);
        } else {
            val_out.set_empty();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::version_edit::FileMetaData;
    use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
    use crate::sst::table_reader::TableReader;
    use crate::types::{InternalKey, ValueType};
    use std::path::Path;
    use std::sync::Arc;

    /// Build an SST file with internal keys in the given range [start, end).
    /// Regression: range-hint rejections shared the prefix-bloom `continue`,
    /// so a seek into a gap in the hinted range visited every remaining file
    /// of the level instead of stopping at the first one past the range.
    #[test]
    fn range_hint_rejection_stops_the_file_scan() {
        let dir = tempfile::tempdir().unwrap();
        let files: Vec<TableFile> = (0..100)
            .filter(|i| !(10..12).contains(i))
            .map(|i| build_sst(dir.path(), i as u64, i, i + 1))
            .collect();
        let hinted = || {
            LevelIterator::new(files.clone())
                .with_range_hints(Some(b"key_000010".to_vec()), Some(b"key_000011".to_vec()))
        };
        let filtered = |f: &dyn Fn()| {
            FILES_FILTERED.with(|n| n.set(0));
            f();
            FILES_FILTERED.with(|n| n.get())
        };
        let target = InternalKey::new(b"key_000010", 100, ValueType::Value);

        let forward = filtered(&|| {
            let mut it = hinted();
            it.seek_to(target.as_bytes());
            assert!(it.next().is_none());
        });
        assert!(forward <= 2, "forward seek filtered {forward} files");

        let backward = filtered(&|| {
            let mut it = hinted();
            it.seek_for_prev(target.as_bytes());
            assert!(it.current().is_none());
        });
        assert!(backward <= 2, "backward seek filtered {backward} files");
    }

    fn build_sst(dir: &Path, file_num: u64, start: usize, end: usize) -> TableFile {
        let path = dir.join(format!("{:06}.sst", file_num));
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

        for i in start..end {
            let uk = format!("key_{:06}", i);
            let ik = InternalKey::new(uk.as_bytes(), 100, ValueType::Value);
            let val = format!("value_{}", i);
            if i == start {
                smallest = ik.as_bytes().to_vec();
            }
            largest = ik.as_bytes().to_vec();
            builder.add(ik.as_bytes(), val.as_bytes()).unwrap();
        }
        builder.finish().unwrap();

        let file_size = std::fs::metadata(&path).unwrap().len();
        let reader = Arc::new(TableReader::open(&path).unwrap());
        TableFile {
            meta: FileMetaData {
                number: file_num,
                file_size,
                smallest_key: smallest,
                largest_key: largest,
                has_range_deletions: false,
            },
            reader,
        }
    }

    /// Build an SST file with prefix bloom enabled.
    fn build_sst_with_prefix_bloom(
        dir: &Path,
        file_num: u64,
        start: usize,
        end: usize,
        prefix_len: usize,
    ) -> TableFile {
        let path = dir.join(format!("{:06}.sst", file_num));
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: 10,
                prefix_len,
                ..Default::default()
            },
        )
        .unwrap();

        let mut smallest = Vec::new();
        let mut largest = Vec::new();

        for i in start..end {
            let uk = format!("key_{:06}", i);
            let ik = InternalKey::new(uk.as_bytes(), 100, ValueType::Value);
            let val = format!("value_{}", i);
            if i == start {
                smallest = ik.as_bytes().to_vec();
            }
            largest = ik.as_bytes().to_vec();
            builder.add(ik.as_bytes(), val.as_bytes()).unwrap();
        }
        builder.finish().unwrap();

        let file_size = std::fs::metadata(&path).unwrap().len();
        let reader = Arc::new(TableReader::open(&path).unwrap());
        TableFile {
            meta: FileMetaData {
                number: file_num,
                file_size,
                smallest_key: smallest,
                largest_key: largest,
                has_range_deletions: false,
            },
            reader,
        }
    }

    #[test]
    fn test_empty_level() {
        let mut iter = LevelIterator::new(vec![]);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let tf = build_sst(dir.path(), 1, 0, 10);

        // Collect via LevelIterator
        let mut level_iter = LevelIterator::new(vec![tf.clone()]);
        let level_entries: Vec<_> = (&mut level_iter).collect();

        // Collect via direct TableIterator
        let mut table_iter = TableIterator::new(tf.reader.clone());
        let table_entries: Vec<_> = (&mut table_iter).collect();

        assert_eq!(level_entries, table_entries);
        assert_eq!(level_entries.len(), 10);
    }

    #[test]
    fn test_multiple_files_concatenation() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![
            build_sst(dir.path(), 1, 0, 10),
            build_sst(dir.path(), 2, 10, 20),
            build_sst(dir.path(), 3, 20, 30),
        ];

        let mut iter = LevelIterator::new(files);
        let entries: Vec<_> = (&mut iter).collect();

        assert_eq!(entries.len(), 30);
        // Verify sorted order
        for i in 1..entries.len() {
            assert!(entries[i].0 > entries[i - 1].0, "not sorted at index {}", i);
        }
        // Verify first and last user keys
        let first_uk = user_key_from_internal(&entries[0].0);
        let last_uk = user_key_from_internal(&entries[29].0);
        assert_eq!(first_uk, b"key_000000");
        assert_eq!(last_uk, b"key_000029");
    }

    #[test]
    fn test_seek_to_exact_key_in_middle_file() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![
            build_sst(dir.path(), 1, 0, 10),
            build_sst(dir.path(), 2, 10, 20),
            build_sst(dir.path(), 3, 20, 30),
        ];

        let target = InternalKey::new(b"key_000015", 100, ValueType::Value);
        let mut iter = LevelIterator::new(files);
        iter.seek_impl(&target.into_bytes());

        let entry = iter.next().unwrap();
        let uk = user_key_from_internal(&entry.0);
        assert_eq!(uk, b"key_000015");
    }

    #[test]
    fn test_seek_between_files() {
        let dir = tempfile::tempdir().unwrap();
        // Files: [0,10), [20,30) — gap at [10,20)
        let files = vec![
            build_sst(dir.path(), 1, 0, 10),
            build_sst(dir.path(), 2, 20, 30),
        ];

        // Seek to key_000015, which is between the two files
        let target = InternalKey::new(b"key_000015", 100, ValueType::Value);
        let mut iter = LevelIterator::new(files);
        iter.seek_impl(&target.into_bytes());

        let entry = iter.next().unwrap();
        let uk = user_key_from_internal(&entry.0);
        assert_eq!(uk, b"key_000020"); // First key in second file
    }

    #[test]
    fn test_seek_past_all_files() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![
            build_sst(dir.path(), 1, 0, 10),
            build_sst(dir.path(), 2, 10, 20),
        ];

        let target = InternalKey::new(b"zzz", 100, ValueType::Value);
        let mut iter = LevelIterator::new(files);
        iter.seek_impl(&target.into_bytes());

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_seek_before_all_files() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![
            build_sst(dir.path(), 1, 10, 20),
            build_sst(dir.path(), 2, 20, 30),
        ];

        let target = InternalKey::new(b"aaa", 100, ValueType::Value);
        let mut iter = LevelIterator::new(files);
        iter.seek_impl(&target.into_bytes());

        let entry = iter.next().unwrap();
        let uk = user_key_from_internal(&entry.0);
        assert_eq!(uk, b"key_000010"); // First key in first file
    }

    #[test]
    fn test_file_boundary_crossing() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![
            build_sst(dir.path(), 1, 0, 3),
            build_sst(dir.path(), 2, 3, 6),
        ];

        let mut iter = LevelIterator::new(files);
        let entries: Vec<_> = (&mut iter).collect();

        assert_eq!(entries.len(), 6);
        // Keys should be contiguous across the boundary
        let uks: Vec<_> = entries
            .iter()
            .map(|(k, _)| user_key_from_internal(k).to_vec())
            .collect();
        assert_eq!(uks[2], b"key_000002");
        assert_eq!(uks[3], b"key_000003"); // crosses file boundary
    }

    #[test]
    fn test_range_filter_skips_files() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![
            build_sst(dir.path(), 1, 0, 10),  // keys 000000..000009
            build_sst(dir.path(), 2, 10, 20), // keys 000010..000019
            build_sst(dir.path(), 3, 20, 30), // keys 000020..000029
        ];

        // Only want keys in range [key_000010, key_000020)
        let mut iter = LevelIterator::new(files)
            .with_range_hints(Some(b"key_000010".to_vec()), Some(b"key_000020".to_vec()));

        let entries: Vec<_> = (&mut iter).collect();
        // File 1 (000000..000009) should be skipped (largest < start)
        // File 2 (000010..000019) should be included
        // File 3 (000020..000029) should be skipped (smallest >= end)
        assert_eq!(entries.len(), 10);
        let first_uk = user_key_from_internal(&entries[0].0);
        let last_uk = user_key_from_internal(&entries[9].0);
        assert_eq!(first_uk, b"key_000010");
        assert_eq!(last_uk, b"key_000019");
    }

    #[test]
    fn test_prefix_bloom_filter_skips_files() {
        let dir = tempfile::tempdir().unwrap();

        // prefix_len=5 means prefixes are first 5 bytes of user key
        // "key_0" is the prefix for keys like key_000000..key_099999
        // "key_1" would be a different prefix (but our keys don't have it)
        let files = vec![
            build_sst_with_prefix_bloom(dir.path(), 1, 0, 10, 5),
            build_sst_with_prefix_bloom(dir.path(), 2, 10, 20, 5),
        ];

        // All keys have prefix "key_0", so querying with "key_0" should find all
        let mut iter = LevelIterator::new(files.clone()).with_prefix(b"key_0".to_vec());
        let entries: Vec<_> = (&mut iter).collect();
        assert_eq!(entries.len(), 20);

        // "xxxx_" prefix should match nothing (bloom filter should reject)
        let mut iter = LevelIterator::new(files).with_prefix(b"xxxx_".to_vec());
        let entries: Vec<_> = (&mut iter).collect();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn empty_or_inverted_range_hints_skip_spanning_files() {
        use super::super::merge::SeekableIterator;

        let dir = tempfile::tempdir().unwrap();
        let files = vec![build_sst(dir.path(), 1, 0, 30)];

        for (start, end) in [
            (b"key_000015".to_vec(), b"key_000015".to_vec()),
            (b"key_000020".to_vec(), b"key_000010".to_vec()),
        ] {
            let mut iter =
                LevelIterator::new(files.clone()).with_range_hints(Some(start), Some(end));
            assert!(iter.next().is_none());
            iter.seek_to_last();
            assert!(iter.current().is_none());
            let target = InternalKey::new(b"key_000015", 100, ValueType::Value);
            iter.seek_for_prev(target.as_bytes());
            assert!(iter.current().is_none());
        }
    }

    #[test]
    fn upper_bound_skips_out_of_bound_files_on_backward_seeks() {
        use super::super::merge::SeekableIterator;
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};

        let dir = tempfile::tempdir().unwrap();
        let file1 = build_sst(dir.path(), 1, 0, 10);
        let file2 = build_sst(dir.path(), 2, 10, 20);

        // Corrupt file 2's first data block on disk; upper_bound = key_000010
        // excludes the entire file, so backward seeks must skip it without I/O.
        let path2 = dir.path().join(format!("{:06}.sst", 2));
        let handle0 = file2.reader.cached_index_entries().unwrap()[0].handle;
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path2)
                .unwrap();
            let crc_pos = handle0.offset + handle0.size + 1;
            f.seek(SeekFrom::Start(crc_pos)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Start(crc_pos)).unwrap();
            f.write_all(&[b[0].wrapping_add(1)]).unwrap();
        }

        let mut iter = LevelIterator::new(vec![file1, file2]).with_fill_cache(false);
        iter.set_bounds(None, Some(b"key_000010"));

        iter.seek_to_last();
        assert!(iter.iter_error().is_none());
        assert_eq!(
            iter.current()
                .map(|(k, _)| user_key_from_internal(&k).to_vec()),
            Some(b"key_000009".to_vec())
        );

        let target = InternalKey::new(b"key_000019", 100, ValueType::Value);
        iter.seek_for_prev(target.as_bytes());
        assert!(iter.iter_error().is_none());
        assert_eq!(
            iter.current()
                .map(|(k, _)| user_key_from_internal(&k).to_vec()),
            Some(b"key_000009".to_vec())
        );
    }

    #[test]
    fn latched_error_persists_across_seeks() {
        use super::super::merge::SeekableIterator;
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};

        let dir = tempfile::tempdir().unwrap();
        let file1 = build_sst(dir.path(), 1, 0, 10);
        let file2 = build_sst(dir.path(), 2, 10, 20);

        // Corrupt file 1's first data block while leaving file 2 intact.
        let path1 = dir.path().join(format!("{:06}.sst", 1));
        let handle0 = file1.reader.cached_index_entries().unwrap()[0].handle;
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path1)
                .unwrap();
            let crc_pos = handle0.offset + handle0.size + 1;
            f.seek(SeekFrom::Start(crc_pos)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Start(crc_pos)).unwrap();
            f.write_all(&[b[0].wrapping_add(1)]).unwrap();
        }

        let mut iter = LevelIterator::new(vec![file1, file2]).with_fill_cache(false);
        assert!(iter.next().is_none());
        assert!(iter.iter_error().is_some());

        // Re-seeking into intact file 2 must not clear the latched error.
        let target = InternalKey::new(b"key_000015", 100, ValueType::Value);
        iter.seek_to(target.as_bytes());
        assert!(iter.iter_error().is_some());
        assert!(iter.next().is_none());

        iter.seek_to_last();
        assert!(iter.iter_error().is_some());
        assert!(iter.current().is_none());

        iter.seek_for_prev(target.as_bytes());
        assert!(iter.iter_error().is_some());
        assert!(iter.current().is_none());

        iter.seek_to_first();
        assert!(iter.iter_error().is_some());
        assert!(iter.next().is_none());
    }
}
