//! LevelIterator: a lazy two-level iterator over non-overlapping SST files at a single level.
//!
//! Instead of opening all SST files upfront, it binary-searches the file list on seek
//! and opens one file's TableIterator at a time. This reduces MergingIterator heap size
//! from O(total_files) to O(L0_count + num_levels).

use std::cmp::Ordering;
use std::sync::Arc;

use crate::iterator::merge::SeekableIterator;
use crate::manifest::version::TableFile;
use crate::options::BlockPropertyFilter;
use crate::sst::table_reader::TableIterator;
use crate::types::{LazyValue, compare_internal_key, user_key as user_key_from_internal};

/// A lazy iterator over a sorted, non-overlapping set of SST files (L1+).
///
/// Opens one file's `TableIterator` at a time, advancing to the next file
/// only when the current one is exhausted.
pub struct LevelIterator {
    /// L1+ files, sorted by smallest_key (non-overlapping).
    files: Vec<TableFile>,
    /// Current position in `files`. files.len() = exhausted.
    file_index: usize,
    /// Lazily opened iterator for the file at file_index.
    current_iter: Option<TableIterator>,
    /// Optional prefix for bloom filter pruning.
    prefix_filter: Option<Vec<u8>>,
    /// Range pruning: user key lower bound (inclusive).
    start_hint: Option<Vec<u8>>,
    /// Range pruning: user key upper bound (exclusive).
    end_hint: Option<Vec<u8>>,
    /// Upper bound for iteration (set via set_bounds).
    upper_bound: Option<Vec<u8>>,
    /// Block property filters to pass to each TableIterator.
    block_property_filters: Vec<Arc<dyn BlockPropertyFilter>>,
}

impl LevelIterator {
    /// Create a new LevelIterator over non-overlapping files sorted by key range.
    pub fn new(files: Vec<TableFile>) -> Self {
        Self {
            files,
            file_index: 0,
            current_iter: None,
            prefix_filter: None,
            start_hint: None,
            end_hint: None,
            upper_bound: None,
            block_property_filters: Vec::new(),
        }
    }

    /// Enable prefix bloom filter pruning.
    pub fn with_prefix(mut self, prefix: Vec<u8>) -> Self {
        self.prefix_filter = Some(prefix);
        self
    }

    /// Enable range-based file skipping.
    pub fn with_range_hints(mut self, start: Option<Vec<u8>>, end: Option<Vec<u8>>) -> Self {
        self.start_hint = start;
        self.end_hint = end;
        self
    }

    /// Attach block property filters that will be passed to each TableIterator.
    pub fn with_block_filters(mut self, filters: Vec<Arc<dyn BlockPropertyFilter>>) -> Self {
        self.block_property_filters = filters;
        self
    }

    /// Check whether a file passes all configured filters.
    fn file_passes_filters(&self, tf: &TableFile) -> bool {
        // Range filter: file's largest user key must be >= start_hint
        if let Some(ref start) = self.start_hint {
            let largest_uk = user_key_from_internal(&tf.meta.largest_key);
            if largest_uk < start.as_slice() {
                return false;
            }
        }
        // Range filter: file's smallest user key must be < end_hint
        if let Some(ref end) = self.end_hint {
            let smallest_uk = user_key_from_internal(&tf.meta.smallest_key);
            if smallest_uk >= end.as_slice() {
                return false;
            }
        }
        // Prefix bloom filter
        if let Some(ref prefix) = self.prefix_filter
            && !tf.reader.prefix_may_match(prefix)
        {
            return false;
        }
        true
    }

    /// Seek to the first entry >= target.
    fn seek_impl(&mut self, target: &[u8]) {
        // Binary search: find first file whose largest_key >= target.
        // partition_point returns first index where predicate is false.
        let idx = self.files.partition_point(|tf| {
            compare_internal_key(&tf.meta.largest_key, target) == Ordering::Less
        });
        self.file_index = idx;
        self.current_iter = None;
        self.open_file_and_seek(Some(target));
    }

    /// Open the file at file_index (skipping filtered files) and optionally seek.
    fn open_file_and_seek(&mut self, target: Option<&[u8]>) {
        while self.file_index < self.files.len() {
            let tf = &self.files[self.file_index];
            if !self.file_passes_filters(tf) {
                self.file_index += 1;
                continue;
            }
            // Skip files whose smallest user key >= upper_bound
            if let Some(ref ub) = self.upper_bound {
                let smallest_uk = user_key_from_internal(&tf.meta.smallest_key);
                if smallest_uk >= ub.as_slice() {
                    self.file_index = self.files.len();
                    self.current_iter = None;
                    return;
                }
            }
            let mut table_iter = TableIterator::new(tf.reader.clone());
            if !self.block_property_filters.is_empty() {
                table_iter = table_iter.with_block_filters(self.block_property_filters.clone());
            }
            if let Some(ref ub) = self.upper_bound {
                table_iter.set_bounds(None, Some(ub));
            }
            if let Some(t) = target {
                table_iter.seek(t);
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
        loop {
            // If no iterator is open yet, try to open the current file.
            if self.current_iter.is_none() && self.file_index < self.files.len() {
                self.open_file_and_seek(None);
                self.current_iter.as_ref()?;
            }
            if let Some(ref mut iter) = self.current_iter
                && let Some(entry) = iter.next()
            {
                return Some(entry);
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
        self.current_iter
            .as_ref()
            .and_then(|iter| iter.current())
            .map(|(k, v)| (k, LazyValue::Inline(v)))
    }

    fn prev(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        // Try prev within current table iterator
        if let Some(ref mut iter) = self.current_iter
            && let Some(entry) = iter.prev()
        {
            return Some((entry.0, LazyValue::Inline(entry.1)));
        }
        // Current table exhausted backward — move to previous file
        loop {
            if self.file_index == 0 {
                self.current_iter = None;
                return None;
            }
            self.file_index -= 1;
            let tf = &self.files[self.file_index];
            if !self.file_passes_filters(tf) {
                continue;
            }
            let mut table_iter = TableIterator::new(tf.reader.clone());
            if !self.block_property_filters.is_empty() {
                table_iter = table_iter.with_block_filters(self.block_property_filters.clone());
            }
            if let Some(ref ub) = self.upper_bound {
                table_iter.set_bounds(None, Some(ub));
            }
            table_iter.seek_to_last();
            // Use current() to read without advancing the cursor, so
            // subsequent prev() calls work correctly.
            if let Some(entry) = table_iter.current() {
                self.current_iter = Some(table_iter);
                return Some((entry.0, LazyValue::Inline(entry.1)));
            }
        }
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
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
            if !self.file_passes_filters(tf) {
                continue;
            }
            let mut table_iter = TableIterator::new(tf.reader.clone());
            if !self.block_property_filters.is_empty() {
                table_iter = table_iter.with_block_filters(self.block_property_filters.clone());
            }
            if let Some(ref ub) = self.upper_bound {
                table_iter.set_bounds(None, Some(ub));
            }
            table_iter.seek_for_prev(target);
            if table_iter.current().is_some() {
                self.file_index = try_idx;
                self.current_iter = Some(table_iter);
                return;
            }
        }
        self.file_index = 0;
        self.current_iter = None;
    }

    fn seek_to_first(&mut self) {
        self.file_index = 0;
        self.current_iter = None;
        self.open_file_and_seek(None);
    }

    fn seek_to_last(&mut self) {
        self.current_iter = None;
        // Start from the last file and work backward
        for idx in (0..self.files.len()).rev() {
            let tf = &self.files[idx];
            if !self.file_passes_filters(tf) {
                continue;
            }
            let mut table_iter = TableIterator::new(tf.reader.clone());
            if !self.block_property_filters.is_empty() {
                table_iter = table_iter.with_block_filters(self.block_property_filters.clone());
            }
            if let Some(ref ub) = self.upper_bound {
                table_iter.set_bounds(None, Some(ub));
            }
            table_iter.seek_to_last();
            self.file_index = idx;
            self.current_iter = Some(table_iter);
            return;
        }
    }

    fn next_into(&mut self, key_buf: &mut Vec<u8>, value_buf: &mut Vec<u8>) -> bool {
        loop {
            if self.current_iter.is_none() && self.file_index < self.files.len() {
                self.open_file_and_seek(None);
                if self.current_iter.is_none() {
                    return false;
                }
            }
            if let Some(ref mut iter) = self.current_iter
                && iter.next_into(key_buf, value_buf)
            {
                return true;
            }
            self.current_iter = None;
            self.file_index += 1;
            if self.file_index >= self.files.len() {
                return false;
            }
        }
    }

    fn set_bounds(&mut self, _lower: Option<&[u8]>, upper: Option<&[u8]>) {
        self.upper_bound = upper.map(|b| b.to_vec());
        // Propagate to the currently open table iterator, if any
        if let Some(ref mut iter) = self.current_iter {
            iter.set_bounds(None, upper);
        }
    }

    fn iter_error(&self) -> Option<String> {
        self.current_iter.as_ref().and_then(|it| it.iter_error())
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

        let reader = Arc::new(TableReader::open(&path).unwrap());
        TableFile {
            meta: FileMetaData {
                number: file_num,
                file_size: reader.file_size(),
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
                internal_keys: true,
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

        let reader = Arc::new(TableReader::open(&path).unwrap());
        TableFile {
            meta: FileMetaData {
                number: file_num,
                file_size: reader.file_size(),
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
}
