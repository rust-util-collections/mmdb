use std::cmp::Ordering;
use std::sync::Arc;

use crate::sst::block::{decode_entry_reuse, Block};
use crate::types::{compare_internal_key, user_key, LazyValue};

pub use crate::sst::format::BLOCK_TRAILER_SIZE;

use super::{IndexEntries, IndexEntry, TableReader};

pub struct TableIterator {
    reader: Arc<TableReader>,
    /// Cached index entries (shared across all iterators for this table).
    /// Lazily loaded on first use — `None` until first access.
    index_entries: Option<IndexEntries>,
    /// Current index position (next block to load on forward iteration)
    index_pos: usize,

    // --- Forward cursor (zero-alloc per entry) ---
    /// Current block's raw data (Arc-shared with block cache).
    current_block: Option<Block>,
    /// Byte offset within block data for next entry to decode.
    block_cursor_offset: usize,
    /// End of entry data in current block (start of restart array).
    block_data_end: usize,
    /// Reusable key buffer — prefix compression reuses shared prefix in-place.
    block_cursor_key: Vec<u8>,

    // --- Readahead ---
    /// Number of consecutive sequential block loads.
    sequential_reads: u32,
    /// Previous block's index position (for detecting sequential access).
    prev_block_index: usize,

    // --- Deferred block read (first_key_from_index) ---
    /// True when positioned at first_key from index without loading data block.
    at_first_key_from_index: bool,
    /// The index position for deferred materialization.
    deferred_index_pos: usize,

    // --- Backward iteration (windowed segment decoding) ---
    /// Materialized entries from a restart segment — only populated by seek_for_prev/prev.
    current_block_entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Position within materialized block segment.
    block_pos: usize,
    /// Current restart index within the backward-iteration block (for windowed segment loading).
    current_restart_index: u32,
    /// Block used for backward iteration (kept for segment decoding).
    backward_block: Option<Block>,
    /// Index of the block used for backward iteration.
    backward_block_index: usize,

    // --- Error state ---
    /// Last I/O or corruption error encountered during iteration.
    err: Option<String>,

    // --- Bounds ---
    /// Exclusive upper bound on user keys. When set, entries with
    /// user_key >= upper_bound are skipped to avoid unnecessary I/O.
    upper_bound: Option<Vec<u8>>,

    // --- Block property filters ---
    /// Filters that can skip entire data blocks based on collected properties.
    block_property_filters: Vec<Arc<dyn crate::options::BlockPropertyFilter>>,
}

impl TableIterator {
    pub fn new(reader: Arc<TableReader>) -> Self {
        Self {
            reader,
            index_entries: None,
            index_pos: 0,
            current_block: None,
            block_cursor_offset: 0,
            block_data_end: 0,
            block_cursor_key: Vec::new(),
            sequential_reads: 0,
            prev_block_index: usize::MAX,
            at_first_key_from_index: false,
            deferred_index_pos: 0,
            current_block_entries: Vec::new(),
            block_pos: 0,
            current_restart_index: 0,
            backward_block: None,
            backward_block_index: usize::MAX,
            err: None,
            upper_bound: None,
            block_property_filters: Vec::new(),
        }
    }

    /// Attach block property filters to this iterator.
    /// Blocks whose properties match a filter's skip criteria will be skipped entirely.
    pub fn with_block_filters(
        mut self,
        filters: Vec<Arc<dyn crate::options::BlockPropertyFilter>>,
    ) -> Self {
        self.block_property_filters = filters;
        self
    }

    /// Ensure index entries are loaded. Returns a reference to them.
    fn ensure_index(&mut self) -> &IndexEntries {
        if self.index_entries.is_none() {
            match self.reader.cached_index_entries() {
                Ok(entries) => self.index_entries = Some(entries),
                Err(e) => {
                    self.err = Some(format!("index decode error: {}", e));
                    // Store an empty sentinel so we don't retry.
                    self.index_entries = Some(Arc::new(Vec::new()));
                }
            }
        }
        self.index_entries.as_ref().unwrap()
    }

    /// Reset cursor state (called when loading a new block for forward iteration).
    fn set_block_for_cursor(&mut self, block: Block) {
        self.block_data_end = block.data_end_offset();
        self.block_cursor_offset = 0;
        self.block_cursor_key.clear();
        self.current_block = Some(block);
        // Invalidate materialized entries
        self.current_block_entries.clear();
        self.block_pos = 0;
    }

    fn reset_positioning_state(&mut self) {
        self.current_block = None;
        self.block_cursor_offset = 0;
        self.block_data_end = 0;
        self.block_cursor_key.clear();
        self.at_first_key_from_index = false;
        self.deferred_index_pos = 0;
        self.current_block_entries.clear();
        self.block_pos = 0;
        self.current_restart_index = 0;
        self.backward_block = None;
        self.backward_block_index = usize::MAX;
    }

    fn block_properties_should_skip(&self, entry: &IndexEntry) -> bool {
        if self.block_property_filters.is_empty() {
            return false;
        }
        self.block_property_filters.iter().any(|filter| {
            let filter_name = filter.name().as_bytes();
            entry
                .properties
                .iter()
                .any(|(name, data)| name.as_slice() == filter_name && filter.should_skip(data))
        })
    }

    fn block_exceeds_upper_bound(&self, entry: &IndexEntry) -> bool {
        self.upper_bound.as_ref().is_some_and(|ub| {
            entry
                .first_key
                .as_ref()
                .is_some_and(|fk| user_key(fk) >= ub.as_slice())
        })
    }

    /// Materialize the deferred block: load the data block for deferred_index_pos.
    fn materialize_deferred_block(&mut self) {
        if let Some(ref index_entries) = self.index_entries {
            match self
                .reader
                .read_block_cached(&index_entries[self.deferred_index_pos].handle)
            {
                Ok(data) => match Block::new(data) {
                    Ok(block) => {
                        self.set_block_for_cursor(block);
                    }
                    Err(e) => {
                        self.err = Some(format!("block decode error: {e}"));
                    }
                },
                Err(e) => {
                    self.err = Some(format!("block read error: {e}"));
                }
            }
        }
    }

    /// Decode the next entry from the current block using the cursor.
    /// Decode the next entry from the current block, returning a lazy value
    /// reference into the block's Arc data (zero-copy for the value).
    fn cursor_next_lazy(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        let block = self.current_block.as_ref()?;
        if self.block_cursor_offset >= self.block_data_end {
            return None;
        }
        let data = block.data();
        let (value_start, value_len, next_offset) =
            decode_entry_reuse(data, self.block_cursor_offset, &mut self.block_cursor_key)?;
        // Check upper bound before returning entry
        if let Some(ref ub) = self.upper_bound
            && user_key(&self.block_cursor_key) >= ub.as_slice()
        {
            return None;
        }
        self.block_cursor_offset = next_offset;
        let lazy_val = LazyValue::BlockRef {
            data: block.data_arc().clone(),
            offset: value_start as u32,
            len: value_len as u32,
        };
        Some((self.block_cursor_key.clone(), lazy_val))
    }

    /// Convenience wrapper that materializes the lazy value for backward compat.
    fn cursor_next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let (k, lv) = self.cursor_next_lazy()?;
        Some((k, lv.into_vec()))
    }

    /// Seek to the first entry >= target using the index block for O(log N) lookup.
    pub fn seek(&mut self, target: &[u8]) {
        self.ensure_index();
        self.reset_positioning_state();
        let index_entries = self.index_entries.as_ref().unwrap();

        // Quick check: if target > file's largest key, mark exhausted.
        if let Some(last) = index_entries.last()
            && compare_internal_key(target, &last.separator_key) == Ordering::Greater
        {
            self.index_pos = index_entries.len();
            return;
        }

        // Binary search index entries to find the first block that may contain target
        let idx = index_entries.partition_point(|entry| {
            compare_internal_key(&entry.separator_key, target) == Ordering::Less
        });

        self.index_pos = idx;

        // Load the found block and seek within it using the cursor
        while self.index_pos < index_entries.len() {
            let entry = &index_entries[self.index_pos];
            if self.block_exceeds_upper_bound(entry) {
                self.index_pos = index_entries.len();
                return;
            }
            self.index_pos += 1;
            if self.block_properties_should_skip(entry) {
                continue;
            }

            // Deferred block read: if target <= first_key, position without I/O
            if let Some(ref first_key) = entry.first_key
                && compare_internal_key(target, first_key) != Ordering::Greater
            {
                self.at_first_key_from_index = true;
                self.deferred_index_pos = self.index_pos - 1;
                self.block_cursor_key.clear();
                self.block_cursor_key.extend_from_slice(first_key);
                self.current_block = None;
                return;
            }

            match self.reader.read_block_cached(&entry.handle) {
                Ok(data) => match Block::new(data) {
                    Ok(block) => {
                        self.seek_within_block(block, target, compare_internal_key);
                    }
                    Err(e) => {
                        self.err = Some(format!("block decode error in seek: {e}"));
                    }
                },
                Err(e) => {
                    self.err = Some(format!("block read error in seek: {e}"));
                }
            }
            return;
        }
    }

    /// Position the cursor at the first entry >= `target` within `block`.
    fn seek_within_block<F: Fn(&[u8], &[u8]) -> Ordering>(
        &mut self,
        block: Block,
        target: &[u8],
        compare: F,
    ) {
        let data_end = block.data_end_offset();
        let block_data = block.data();
        let num_restarts = block.num_restarts();

        // Binary search on restart points to narrow the scan range
        let mut left = 0u32;
        let mut right = num_restarts;
        let mut tmp_key = Vec::new();
        while left < right {
            let mid = left + (right - left) / 2;
            let rp_offset = data_end + (mid as usize) * 4;
            let rp = u32::from_le_bytes(block_data[rp_offset..rp_offset + 4].try_into().unwrap())
                as usize;
            tmp_key.clear();
            match decode_entry_reuse(block_data, rp, &mut tmp_key) {
                Some(_) => {
                    if compare(&tmp_key, target) == Ordering::Less {
                        left = mid + 1;
                    } else {
                        right = mid;
                    }
                }
                None => right = mid,
            }
        }

        // Linear scan from restart point before `left`
        let start = if left > 0 {
            let rp_offset = data_end + ((left - 1) as usize) * 4;
            u32::from_le_bytes(block_data[rp_offset..rp_offset + 4].try_into().unwrap()) as usize
        } else {
            0
        };

        // Scan entries, saving key state before each decode so we can restore
        // the prefix-compressed key buffer without a second scan.
        self.block_cursor_key.clear();
        let mut prev_key_snapshot: Vec<u8> = Vec::new();
        let mut offset = start;
        while offset < data_end {
            let entry_offset = offset;
            // Save key buffer state before decoding this entry
            prev_key_snapshot.clear();
            prev_key_snapshot.extend_from_slice(&self.block_cursor_key);

            match decode_entry_reuse(block_data, offset, &mut self.block_cursor_key) {
                Some((_, _, next_off)) => {
                    if compare(&self.block_cursor_key, target) != Ordering::Less {
                        // Found first entry >= target.
                        // Restore key buffer to pre-decode state so cursor_next()
                        // will re-decode this entry correctly.
                        self.block_cursor_key.clear();
                        self.block_cursor_key.extend_from_slice(&prev_key_snapshot);
                        self.block_cursor_offset = entry_offset;
                        self.block_data_end = data_end;
                        self.current_block = Some(block);
                        return;
                    }
                    offset = next_off;
                }
                None => break,
            }
        }
        // All entries < target
        self.block_cursor_offset = data_end;
        self.block_data_end = data_end;
        self.block_cursor_key.clear();
        self.current_block = Some(block);
    }

    /// Seek to the last entry <= target using compare_internal_key ordering.
    /// After this call, the iterator is positioned on the found entry (or exhausted
    /// if no entry <= target exists).
    ///
    /// Uses windowed segment decoding: only decodes the restart segment containing
    /// the target entry, not the entire block.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.ensure_index();
        self.reset_positioning_state();
        let index_entries = self.index_entries.as_ref().unwrap();

        let idx = index_entries.partition_point(|entry| {
            compare_internal_key(&entry.separator_key, target) == Ordering::Less
        });

        // Try the block at `idx` first, then fall back to previous blocks.
        let mut found = false;
        let mut try_idx = idx;

        loop {
            if try_idx >= index_entries.len() {
                if try_idx == 0 {
                    break;
                }
                try_idx -= 1;
                continue;
            }

            let entry = &index_entries[try_idx];
            if self.block_properties_should_skip(entry) {
                if try_idx == 0 {
                    break;
                }
                try_idx -= 1;
                continue;
            }
            let handle = entry.handle;

            let block_result = self.reader.read_block_cached(&handle).and_then(Block::new);
            match block_result {
                Err(e) => {
                    self.err = Some(format!("block read error in seek_for_prev: {e}"));
                    break;
                }
                Ok(block) => match block.seek_for_prev_by(target, compare_internal_key) {
                    Some((found_key, _found_val)) => {
                        // Determine which restart segment this entry belongs to
                        // and decode all entries from that segment to end of block.
                        // Using iter_from_restart (not iter_restart_segment) ensures
                        // that a subsequent next() will see all remaining entries in
                        // this block before advancing to the next block.
                        let restart_idx =
                            self.find_restart_for_key(&block, &found_key, compare_internal_key);
                        let entries_from_restart = block.iter_from_restart(restart_idx);
                        // Find position of found entry within the decoded range
                        let pos_in_entries = entries_from_restart
                            .iter()
                            .rposition(|(k, _)| {
                                compare_internal_key(k, target) != Ordering::Greater
                            })
                            .unwrap_or(0);

                        self.index_pos = try_idx + 1;
                        self.current_block_entries = entries_from_restart;
                        self.block_pos = pos_in_entries;
                        self.current_restart_index = restart_idx;
                        self.backward_block = Some(block);
                        self.backward_block_index = try_idx;
                        found = true;
                        break;
                    }
                    None => {
                        // No entry <= target in this block; try previous
                    }
                }, // Ok(block) => match seek_for_prev_by
            } // match block_result

            if try_idx == 0 {
                break;
            }
            try_idx -= 1;
        }

        if !found {
            self.index_pos = index_entries.len();
            self.current_block_entries.clear();
            self.block_pos = 0;
            self.backward_block = None;
        }
    }

    /// Find the restart index that contains a given key.
    /// Uses O(log R) binary search with O(1) per probe (single entry decode).
    fn find_restart_for_key<F: Fn(&[u8], &[u8]) -> Ordering>(
        &self,
        block: &Block,
        key: &[u8],
        compare: F,
    ) -> u32 {
        let num = block.num_restarts();
        if num <= 1 {
            return 0;
        }
        // Binary search: find last restart point whose first key <= key
        let mut left = 0u32;
        let mut right = num;
        while left < right {
            let mid = left + (right - left) / 2;
            if let Some(first_key) = block.first_key_at_restart(mid) {
                if compare(&first_key, key) != Ordering::Greater {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            } else {
                right = mid;
            }
        }
        left.saturating_sub(1)
    }

    /// Move to the previous entry. Returns the entry at the new position,
    /// or None if we've moved before the first entry.
    ///
    /// Uses windowed segment decoding: when crossing restart segment boundaries,
    /// only loads the previous segment (not the entire block).
    pub fn prev(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        // If we have a previous entry in the current segment, just decrement
        if self.block_pos > 0 {
            self.block_pos -= 1;
            return Some(self.current_block_entries[self.block_pos].clone());
        }

        // Try loading the previous restart segment within the same block
        if self.current_restart_index > 0
            && let Some(ref block) = self.backward_block
        {
            self.current_restart_index -= 1;
            self.current_block_entries = block.iter_restart_segment(self.current_restart_index);
            if !self.current_block_entries.is_empty() {
                self.block_pos = self.current_block_entries.len() - 1;
                return Some(self.current_block_entries[self.block_pos].clone());
            }
        }

        // Need to load the previous block's last segment.
        let current_block_index = if self.backward_block_index < usize::MAX {
            self.backward_block_index
        } else if self.index_pos > 0 {
            self.index_pos - 1
        } else {
            return None;
        };

        if current_block_index == 0 {
            return None; // Already at first block
        }

        self.ensure_index();
        let index_entries = self.index_entries.as_ref().unwrap();
        let mut prev_block_index = current_block_index - 1;

        loop {
            let entry = &index_entries[prev_block_index];
            if self.block_properties_should_skip(entry) {
                if prev_block_index == 0 {
                    return None;
                }
                prev_block_index -= 1;
                continue;
            }

            match self.reader.read_block_cached(&entry.handle) {
                Ok(data) => match Block::new(data) {
                    Ok(block) => {
                        let last_restart = block.num_restarts().saturating_sub(1);
                        self.current_block_entries = block.iter_restart_segment(last_restart);
                        if self.current_block_entries.is_empty() {
                            if prev_block_index == 0 {
                                return None;
                            }
                            prev_block_index -= 1;
                            continue;
                        }
                        self.block_pos = self.current_block_entries.len() - 1;
                        self.index_pos = prev_block_index + 1;
                        self.current_restart_index = last_restart;
                        self.backward_block = Some(block);
                        self.backward_block_index = prev_block_index;
                        return Some(self.current_block_entries[self.block_pos].clone());
                    }
                    Err(e) => {
                        self.err = Some(format!("block decode error in prev: {e}"));
                    }
                },
                Err(e) => {
                    self.err = Some(format!("block read error in prev: {e}"));
                }
            }

            return None;
        }
    }

    /// Return the current entry without advancing, or None if not positioned.
    pub fn current(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.block_pos < self.current_block_entries.len() {
            Some(self.current_block_entries[self.block_pos].clone())
        } else {
            None
        }
    }

    fn load_next_block(&mut self) -> bool {
        self.ensure_index();
        let index_entries = self.index_entries.as_ref().unwrap();
        while self.index_pos < index_entries.len() {
            let block_idx = self.index_pos;

            let entry = &index_entries[block_idx];

            // Skip blocks whose first_key user key >= upper_bound
            if self.block_exceeds_upper_bound(entry) {
                self.index_pos = index_entries.len();
                return false;
            }

            // Skip blocks based on block property filters
            if self.block_properties_should_skip(entry) {
                self.index_pos += 1;
                continue;
            }

            let handle = index_entries[self.index_pos].handle;
            self.index_pos += 1;

            // Detect sequential access and trigger readahead
            if block_idx == self.prev_block_index.wrapping_add(1) {
                self.sequential_reads += 1;
                if self.sequential_reads >= 2 {
                    self.maybe_readahead(index_entries, block_idx);
                }
            } else {
                self.sequential_reads = 0;
            }
            self.prev_block_index = block_idx;

            match self.reader.read_block_cached(&handle) {
                Ok(data) => match Block::new(data) {
                    Ok(block) => {
                        if block.data_end_offset() > 0 {
                            self.set_block_for_cursor(block);
                            return true;
                        }
                    }
                    Err(e) => {
                        self.err = Some(format!("block decode error at index {block_idx}: {e}"));
                        return false;
                    }
                },
                Err(e) => {
                    self.err = Some(format!("block read error at index {block_idx}: {e}"));
                    return false;
                }
            }
        }
        false
    }

    /// Issue a readahead hint for upcoming blocks.
    fn maybe_readahead(&self, index_entries: &[IndexEntry], current_idx: usize) {
        // Prefetch the next N blocks (adaptive: starts at 2, grows to 8)
        let prefetch_count = (self.sequential_reads as usize).min(8);
        let start = current_idx + 1;
        let end = (start + prefetch_count).min(index_entries.len());
        if start >= end {
            return;
        }

        // Compute the file range covering the upcoming blocks
        let first_handle = index_entries[start].handle;
        let last_handle = index_entries[end - 1].handle;
        let offset = first_handle.offset;
        let len = (last_handle.offset + last_handle.size + BLOCK_TRAILER_SIZE as u64) - offset;

        self.reader.advise_willneed(offset, len);
    }
}

impl Iterator for TableIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // F7: bail immediately if a prior I/O error was recorded
        if self.err.is_some() {
            return None;
        }
        loop {
            // Handle deferred block read: materialize now that we need the value
            if self.at_first_key_from_index {
                self.at_first_key_from_index = false;
                self.materialize_deferred_block();
                if let Some(entry) = self.cursor_next() {
                    return Some(entry);
                }
                // Fall through to load_next_block if materialize failed
            }
            // Try cursor-based path first (forward iteration)
            if let Some(entry) = self.cursor_next() {
                return Some(entry);
            }
            // Try materialized entries (backward iteration positioned us here)
            if self.block_pos < self.current_block_entries.len() {
                let entry = self.current_block_entries[self.block_pos].clone();
                self.block_pos += 1;
                return Some(entry);
            }
            // Load next block via cursor path
            if !self.load_next_block() {
                return None;
            }
        }
    }
}

impl crate::iterator::merge::SeekableIterator for TableIterator {
    fn seek_to(&mut self, target: &[u8]) {
        self.seek(target);
    }

    fn current(&self) -> Option<(Vec<u8>, LazyValue)> {
        TableIterator::current(self).map(|(k, v)| (k, LazyValue::Inline(v)))
    }

    fn prev(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        TableIterator::prev(self).map(|(k, v)| (k, LazyValue::Inline(v)))
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        TableIterator::seek_for_prev(self, target);
    }

    fn seek_to_first(&mut self) {
        // Reset to the beginning of the table
        self.ensure_index();
        self.reset_positioning_state();
        self.index_pos = 0;
    }

    fn seek_to_last(&mut self) {
        self.ensure_index();
        self.reset_positioning_state();
        let index_entries = self.index_entries.as_ref().unwrap();
        if index_entries.is_empty() {
            return;
        }
        // Load only the last restart segment of the last block
        let mut last_idx = index_entries.len() - 1;
        while self.block_properties_should_skip(&index_entries[last_idx]) {
            if last_idx == 0 {
                return;
            }
            last_idx -= 1;
        }
        let handle = index_entries[last_idx].handle;
        match self.reader.read_block_cached(&handle) {
            Ok(data) => match Block::new(data) {
                Ok(block) => {
                    let last_restart = block.num_restarts().saturating_sub(1);
                    self.current_block_entries = block.iter_restart_segment(last_restart);
                    if !self.current_block_entries.is_empty() {
                        self.block_pos = self.current_block_entries.len() - 1;
                        self.index_pos = last_idx + 1;
                        self.current_restart_index = last_restart;
                        self.backward_block = Some(block);
                        self.backward_block_index = last_idx;
                    }
                }
                Err(e) => {
                    self.err = Some(format!("block decode error in seek_to_last: {e}"));
                }
            },
            Err(e) => {
                self.err = Some(format!("block read error in seek_to_last: {e}"));
            }
        }
    }

    fn next_into(&mut self, key_buf: &mut Vec<u8>, value_buf: &mut Vec<u8>) -> bool {
        // Bail immediately if a prior I/O error was recorded
        if self.err.is_some() {
            return false;
        }
        // Handle deferred block read
        if self.at_first_key_from_index {
            self.at_first_key_from_index = false;
            self.materialize_deferred_block();
        }
        loop {
            // Try cursor-based path (forward iteration)
            if let Some(ref block) = self.current_block
                && self.block_cursor_offset < self.block_data_end
            {
                let data = block.data();
                if let Some((vs, vl, next)) =
                    decode_entry_reuse(data, self.block_cursor_offset, &mut self.block_cursor_key)
                {
                    // Check upper bound before returning entry
                    if let Some(ref ub) = self.upper_bound
                        && user_key(&self.block_cursor_key) >= ub.as_slice()
                    {
                        return false;
                    }
                    self.block_cursor_offset = next;
                    key_buf.clear();
                    key_buf.extend_from_slice(&self.block_cursor_key);
                    value_buf.clear();
                    value_buf.extend_from_slice(&data[vs..vs + vl]);
                    return true;
                }
            }
            // Try materialized entries (backward iteration)
            if self.block_pos < self.current_block_entries.len() {
                let (ref k, ref v) = self.current_block_entries[self.block_pos];
                // Check upper bound before returning entry
                if let Some(ref ub) = self.upper_bound
                    && user_key(k) >= ub.as_slice()
                {
                    return false;
                }
                key_buf.clear();
                key_buf.extend_from_slice(k);
                value_buf.clear();
                value_buf.extend_from_slice(v);
                self.block_pos += 1;
                return true;
            }
            if !self.load_next_block() {
                return false;
            }
        }
    }

    fn prefetch_first_block(&mut self) {
        self.ensure_index();
        if let Some(index) = self.index_entries.as_ref()
            && let Some(entry) = index.first()
        {
            self.reader.advise_willneed(
                entry.handle.offset,
                entry.handle.size + BLOCK_TRAILER_SIZE as u64,
            );
        }
    }

    fn set_bounds(&mut self, _lower: Option<&[u8]>, upper: Option<&[u8]>) {
        self.upper_bound = upper.map(|b| b.to_vec());
    }

    fn iter_error(&self) -> Option<String> {
        self.err.clone()
    }

    fn next_lazy(&mut self, key_buf: &mut Vec<u8>) -> Option<LazyValue> {
        // F7: bail immediately if a prior I/O error was recorded
        if self.err.is_some() {
            return None;
        }
        // F6: materialize deferred block if positioned at first_key from index
        if self.at_first_key_from_index {
            self.at_first_key_from_index = false;
            self.materialize_deferred_block();
            if self.err.is_some() {
                return None;
            }
        }
        loop {
            // Forward cursor path (hot path)
            if let Some(ref block) = self.current_block
                && self.block_cursor_offset < self.block_data_end
            {
                let data = block.data();
                let result =
                    decode_entry_reuse(data, self.block_cursor_offset, &mut self.block_cursor_key);
                if let Some((value_start, value_len, next_offset)) = result {
                    // Check upper bound
                    if let Some(ref ub) = self.upper_bound
                        && user_key(&self.block_cursor_key) >= ub.as_slice()
                    {
                        return None;
                    }
                    self.block_cursor_offset = next_offset;
                    key_buf.clear();
                    key_buf.extend_from_slice(&self.block_cursor_key);
                    return Some(LazyValue::BlockRef {
                        data: block.data_arc().clone(),
                        offset: value_start as u32,
                        len: value_len as u32,
                    });
                }
            }
            // Materialized entries path (backward iteration)
            if self.block_pos < self.current_block_entries.len() {
                let (ref k, ref v) = self.current_block_entries[self.block_pos];
                if let Some(ref ub) = self.upper_bound
                    && user_key(k) >= ub.as_slice()
                {
                    return None;
                }
                key_buf.clear();
                key_buf.extend_from_slice(k);
                let lv = LazyValue::Inline(v.clone());
                self.block_pos += 1;
                return Some(lv);
            }
            // Try loading next block
            if !self.load_next_block() {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use super::*;
    use crate::options::{BlockPropertyCollector, BlockPropertyFilter};
    use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
    use crate::types::{InternalKeyRef, ValueType};

    fn build_test_table(dir: &Path, count: usize) -> PathBuf {
        let path = dir.join("test.sst");
        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        for i in 0..count {
            let key = format!("key_{:06}", i);
            let val = format!("value_{}", i);
            builder.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        builder.finish().unwrap();
        path
    }

    #[test]
    fn test_table_read_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 100);

        let reader = TableReader::open(&path).unwrap();
        let entries = reader.iter().unwrap();
        assert_eq!(entries.len(), 100);

        for (i, (k, v)) in entries.iter().enumerate() {
            assert_eq!(k, format!("key_{:06}", i).as_bytes());
            assert_eq!(v, format!("value_{}", i).as_bytes());
        }
    }

    #[test]
    fn test_table_point_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 100);

        let reader = TableReader::open(&path).unwrap();

        let val = reader.get(b"key_000050").unwrap();
        assert_eq!(val, Some(b"value_50".to_vec()));

        let val = reader.get(b"key_000000").unwrap();
        assert_eq!(val, Some(b"value_0".to_vec()));

        let val = reader.get(b"key_000099").unwrap();
        assert_eq!(val, Some(b"value_99".to_vec()));

        let val = reader.get(b"key_999999").unwrap();
        assert_eq!(val, None);

        let val = reader.get(b"aaa").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_table_large() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 10000);

        let reader = TableReader::open(&path).unwrap();

        for i in (0..10000).step_by(100) {
            let key = format!("key_{:06}", i);
            let val = format!("value_{}", i);
            assert_eq!(
                reader.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "failed at key {}",
                i
            );
        }
    }

    #[test]
    fn test_bloom_filter_used() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 100);

        let reader = TableReader::open(&path).unwrap();
        assert!(reader.filter_data.is_some());

        let val = reader.get(b"nonexistent_key_12345").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_internal_key_lookup() {
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("internal.sst");

        // Build SST with internal keys
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: 0, // disable bloom for this test
                ..Default::default()
            },
        )
        .unwrap();

        // user_key "aaa" at seq 5 (Value), seq 3 (Deletion)
        // user_key "bbb" at seq 4 (Value)
        // Internal keys sorted by: user_key ASC, but in lex byte order
        // which for internal keys is user_key prefix then trailer bytes.
        let ik1 = InternalKey::new(b"aaa", 3, ValueType::Deletion);
        let ik2 = InternalKey::new(b"aaa", 5, ValueType::Value);
        let ik3 = InternalKey::new(b"bbb", 4, ValueType::Value);

        // Sort by raw bytes (lex order, as the skiplist would)
        let mut entries = vec![
            (ik1.as_bytes().to_vec(), b"".to_vec()),
            (ik2.as_bytes().to_vec(), b"val_aaa".to_vec()),
            (ik3.as_bytes().to_vec(), b"val_bbb".to_vec()),
        ];
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (k, v) in &entries {
            builder.add(k, v).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();

        // Look up "aaa" at seq 10 — should find seq 5 Value
        let result = reader.get_internal(b"aaa", 10).unwrap();
        assert!(result.is_some());

        // Look up "bbb" at seq 10
        let result = reader.get_internal(b"bbb", 10).unwrap();
        assert_eq!(result, Some(Some(b"val_bbb".to_vec())));

        // Look up "ccc" — not found
        let result = reader.get_internal(b"ccc", 10).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_table_iterator_streaming() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 500);

        let reader = Arc::new(TableReader::open(&path).unwrap());
        let mut iter = TableIterator::new(reader);

        // Collect all entries via the streaming iterator
        let mut count = 0;
        let mut prev_key: Option<Vec<u8>> = None;
        for (k, v) in &mut iter {
            let expected_key = format!("key_{:06}", count);
            let expected_val = format!("value_{}", count);
            assert_eq!(
                k,
                expected_key.as_bytes(),
                "key mismatch at index {}",
                count
            );
            assert_eq!(
                v,
                expected_val.as_bytes(),
                "value mismatch at index {}",
                count
            );

            // Verify keys are in sorted order
            if let Some(ref pk) = prev_key {
                assert!(
                    k.as_slice() > pk.as_slice(),
                    "keys not in order at {}",
                    count
                );
            }
            prev_key = Some(k);
            count += 1;
        }
        assert_eq!(count, 500);
    }

    /// Build a test table with internal keys for seek_for_prev/prev tests.
    fn build_internal_key_table(dir: &Path, count: usize) -> PathBuf {
        use crate::types::InternalKey;

        let path = dir.join("internal_iter.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();

        // Build sorted internal keys: each user key at seq=count-i (descending seq
        // doesn't matter here since each user_key is unique)
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = (0..count)
            .map(|i| {
                let uk = format!("key_{:06}", i);
                let ik = InternalKey::new(uk.as_bytes(), (count - i) as u64, ValueType::Value);
                let val = format!("value_{}", i);
                (ik.into_bytes(), val.into_bytes())
            })
            .collect();
        // Internal keys with distinct user_keys are already in correct order since
        // user_key ASC is the primary sort. But let's sort to be safe.
        entries.sort_by(|(a, _), (b, _)| compare_internal_key(a, b));

        for (k, v) in &entries {
            builder.add(k, v).unwrap();
        }
        builder.finish().unwrap();
        path
    }

    #[test]
    fn test_table_iterator_seek_for_prev() {
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = build_internal_key_table(dir.path(), 100);
        let reader = Arc::new(TableReader::open(&path).unwrap());

        // For seek_for_prev, we want to find the last entry for a given user key.
        // Use seq=0, Deletion type to create a key that sorts AFTER all entries
        // for that user key (since lower seq sorts later in internal key order).
        let seek_key =
            |uk: &[u8]| -> Vec<u8> { InternalKey::new(uk, 0, ValueType::Deletion).into_bytes() };
        let extract_uk = |ikey: &[u8]| -> Vec<u8> { InternalKeyRef::new(ikey).user_key().to_vec() };

        // seek_for_prev to exact user key
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"key_000050"));
        let entry = iter.current().unwrap();
        assert_eq!(extract_uk(&entry.0), b"key_000050");
        assert_eq!(entry.1, b"value_50");

        // seek_for_prev to key between entries (key_000050 < target < key_000051)
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"key_000050x"));
        let entry = iter.current().unwrap();
        assert_eq!(extract_uk(&entry.0), b"key_000050");

        // seek_for_prev past all keys
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"zzz"));
        let entry = iter.current().unwrap();
        assert_eq!(extract_uk(&entry.0), b"key_000099");

        // seek_for_prev before all keys
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"aaa"));
        assert!(iter.current().is_none());

        // seek_for_prev to first key
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"key_000000"));
        let entry = iter.current().unwrap();
        assert_eq!(extract_uk(&entry.0), b"key_000000");

        // After seek_for_prev, forward iteration should work
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"key_000050"));
        // Advance block_pos to consume the current entry
        iter.block_pos += 1;
        let next = iter.next();
        assert!(next.is_some());
        assert_eq!(extract_uk(&next.unwrap().0), b"key_000051");
    }

    #[test]
    fn test_table_iterator_prev() {
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = build_internal_key_table(dir.path(), 100);
        let reader = Arc::new(TableReader::open(&path).unwrap());

        let seek_key =
            |uk: &[u8]| -> Vec<u8> { InternalKey::new(uk, 0, ValueType::Deletion).into_bytes() };
        let extract_uk = |ikey: &[u8]| -> Vec<u8> { InternalKeyRef::new(ikey).user_key().to_vec() };

        // Seek to middle, then prev
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"key_000050"));
        assert_eq!(extract_uk(&iter.current().unwrap().0), b"key_000050");

        let prev = iter.prev().unwrap();
        assert_eq!(extract_uk(&prev.0), b"key_000049");

        let prev = iter.prev().unwrap();
        assert_eq!(extract_uk(&prev.0), b"key_000048");

        // Seek to end, prev through several entries
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"zzz"));
        assert_eq!(extract_uk(&iter.current().unwrap().0), b"key_000099");

        let prev = iter.prev().unwrap();
        assert_eq!(extract_uk(&prev.0), b"key_000098");

        // Seek to first key, prev should return None
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"key_000000"));
        assert_eq!(extract_uk(&iter.current().unwrap().0), b"key_000000");
        assert!(iter.prev().is_none());

        // Prev across block boundaries (with 100 entries, there are multiple blocks)
        let mut iter = TableIterator::new(reader.clone());
        iter.seek_for_prev(&seek_key(b"key_000099"));
        // Walk backwards through all entries
        let mut count = 1; // start at 1 for the current entry
        while iter.prev().is_some() {
            count += 1;
        }
        assert_eq!(count, 100, "should be able to prev through all 100 entries");
    }

    #[derive(Default)]
    struct FirstPrefixCollector {
        skip_block: bool,
        seen: bool,
    }

    impl BlockPropertyCollector for FirstPrefixCollector {
        fn add(&mut self, key: &[u8], _value: &[u8]) {
            if self.seen {
                return;
            }
            self.seen = true;
            self.skip_block = InternalKeyRef::new(key).user_key().starts_with(b"a_skip_");
        }

        fn finish_block(&mut self) -> Vec<u8> {
            let result = if self.skip_block {
                b"skip".to_vec()
            } else {
                b"keep".to_vec()
            };
            self.skip_block = false;
            self.seen = false;
            result
        }

        fn name(&self) -> &str {
            "first-prefix"
        }
    }

    struct SkipBlocksFilter;

    impl BlockPropertyFilter for SkipBlocksFilter {
        fn should_skip(&self, properties: &[u8]) -> bool {
            properties == b"skip"
        }

        fn name(&self) -> &str {
            "first-prefix"
        }
    }

    #[test]
    fn test_table_iterator_prev_skips_filtered_blocks() {
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("filtered_prev.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1,
                bloom_bits_per_key: 0,
                block_property_collectors: vec![Box::<FirstPrefixCollector>::default()],
                ..Default::default()
            },
        )
        .unwrap();

        for prefix in ["a_skip", "z_keep"] {
            for i in 0..20 {
                let user_key = format!("{}_{:03}", prefix, i);
                let ikey = InternalKey::new(user_key.as_bytes(), 100 - i, ValueType::Value);
                builder
                    .add(ikey.as_bytes(), format!("value_{prefix}_{i}").as_bytes())
                    .unwrap();
            }
        }
        builder.finish().unwrap();

        let reader = Arc::new(TableReader::open(&path).unwrap());
        let mut iter =
            TableIterator::new(reader).with_block_filters(vec![Arc::new(SkipBlocksFilter)]);
        let seek_key = InternalKey::new(b"zzzz", 0, ValueType::Deletion);
        iter.seek_for_prev(seek_key.as_bytes());

        let mut seen = 0;
        while let Some((key, _)) = iter.current() {
            let user_key = InternalKeyRef::new(&key).user_key().to_vec();
            assert!(
                user_key.starts_with(b"z_keep_"),
                "filtered reverse scan yielded skipped key {:?}",
                String::from_utf8_lossy(&user_key)
            );
            seen += 1;
            if iter.prev().is_none() {
                break;
            }
        }

        assert_eq!(seen, 20);
    }
}
