use std::{cmp::Ordering, sync::Arc};

use smallvec::SmallVec;

use crate::error::Result;
use crate::sst::block::{
    Block, RestartSegmentEntry, decode_point_entry_monotonic, validate_point_ikey,
};
use crate::sst::format::{BlockHandle, DecodedIndexEntry, decode_index_entry};
use crate::types::{
    InternalKey, LazyValue, MAX_SEQUENCE_NUMBER, ValueType, compare_internal_key, user_key,
};
use crate::{Error, options::IterConfig};

pub use crate::sst::format::BLOCK_TRAILER_SIZE;

use super::TableReader;

#[inline]
fn index_entry_at(block: &Block, idx: usize) -> (&[u8], DecodedIndexEntry<'_>) {
    let (sep, val, _) = block
        .decode_restart_entry(idx as u32)
        .expect("validated index leaf entry");
    let entry = decode_index_entry(val).expect("validated index value");
    (sep, entry)
}

fn checked_readahead_range(
    first_handle: BlockHandle,
    last_handle: BlockHandle,
) -> Option<(u64, u64)> {
    if last_handle.offset < first_handle.offset {
        return None;
    }
    let end = last_handle
        .offset
        .checked_add(last_handle.size)?
        .checked_add(BLOCK_TRAILER_SIZE as u64)?;
    Some((first_handle.offset, end.checked_sub(first_handle.offset)?))
}

pub struct TableIterator {
    reader: Arc<TableReader>,
    /// Pre-validated index leaf block (shared zero-copy from `BlockCache`).
    /// Lazily loaded per partition without heap-parsing `IndexEntry` vectors.
    index_block: Option<Block>,
    index_loaded: bool,
    partition: u32,
    /// Current index position (next block to load on forward iteration)
    index_pos: usize,

    // --- Forward cursor (zero-alloc per entry) ---
    /// Current block's raw data (Arc-shared with block cache).
    current_block: Option<Block>,
    /// Byte offset within block data for next entry to decode.
    block_cursor_offset: u32,
    /// End of entry data in current block (start of restart array).
    block_data_end: u32,
    /// Next restart point index (`1..=num_restarts`) ahead of `block_cursor_offset`.
    block_next_restart: u32,
    /// Exclusive end offset of the current forward restart segment.
    block_seg_end: u32,
    /// Byte slice `(offset, len)` of `current_block`'s separator inside `index_block`.
    block_sep_off: u32,
    block_sep_len: u32,
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
    deferred_index_pos: u32,

    // --- Backward iteration (windowed segment decoding) ---
    /// Entry descriptors for the active backward restart segment (zero value copies).
    backward_entries: Vec<RestartSegmentEntry>,
    /// Contiguous decoded key arena referenced by `backward_entries`.
    backward_keys: Vec<u8>,
    /// Value slice `(from_backward_block, offset, len)` for the entry most recently
    /// decoded by `advance_key`, materialized lazily via `materialize_value_into`.
    last_value_loc: (bool, u32, u32),
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
    err: Option<Error>,

    config: Arc<IterConfig>,

    // --- Cache policy ---
    /// Whether cache misses populate the block cache. False for scans that
    /// must not evict hot blocks (compaction, `ReadOptions::fill_cache=false`).
    fill_cache: bool,
}

impl TableIterator {
    pub fn new(reader: Arc<TableReader>) -> Self {
        Self {
            reader,
            index_block: None,
            index_loaded: false,
            partition: 0,
            index_pos: 0,
            current_block: None,
            block_cursor_offset: 0,
            block_data_end: 0,
            block_next_restart: 1,
            block_seg_end: 0,
            block_sep_off: 0,
            block_sep_len: 0,
            block_cursor_key: Vec::new(),
            sequential_reads: 0,
            prev_block_index: usize::MAX,
            at_first_key_from_index: false,
            deferred_index_pos: 0,
            backward_entries: Vec::new(),
            backward_keys: Vec::new(),
            last_value_loc: (false, 0, 0),
            block_pos: 0,
            current_restart_index: 0,
            backward_block: None,
            backward_block_index: usize::MAX,
            err: None,
            config: IterConfig::empty(),
            fill_cache: true,
        }
    }

    /// Set whether cache misses populate the block cache (default: true).
    pub fn with_fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
        self
    }

    /// Attach block property filters to this iterator.
    /// Blocks whose properties match a filter's skip criteria will be skipped entirely.
    #[cfg(test)]
    pub fn with_block_filters(
        mut self,
        filters: Vec<Arc<dyn crate::options::BlockPropertyFilter>>,
    ) -> Self {
        Arc::make_mut(&mut self.config).filters = filters.into();
        self
    }

    fn select_partition(&mut self, partition: usize) {
        let p = partition as u32;
        if self.partition != p {
            self.partition = p;
            self.index_block = None;
            self.index_loaded = false;
            self.prev_block_index = usize::MAX;
            self.index_pos = 0;
        }
        self.ensure_index();
    }

    fn seek_partition(&self, target: &[u8]) -> usize {
        self.reader
            .partitions
            .partition_point(|(sep, _)| sep.compare_internal(target) == Ordering::Less)
    }

    /// Ensure the current partition's validated index leaf block is loaded.
    fn ensure_index(&mut self) {
        if !self.index_loaded {
            self.index_loaded = true;
            match self
                .reader
                .index_leaf_block(self.partition as usize, self.fill_cache)
            {
                Ok(block) => self.index_block = block,
                Err(e) => {
                    self.err = Some(e.context("decoding SST index"));
                    self.index_block = None;
                }
            }
        }
    }

    #[inline]
    fn index_len(&self) -> usize {
        self.index_block
            .as_ref()
            .map_or(0, |b| b.num_restarts() as usize)
    }

    /// Validate entry 0 against the index leaf and reset cursor state for `block`.
    fn set_block_for_cursor(
        &mut self,
        block: Block,
        block_idx: usize,
        ordered: bool,
    ) -> Result<()> {
        if block.data_end_offset() == 0 {
            return Err(Error::corruption("empty SST data block"));
        }
        if let Some(ref index_block) = self.index_block {
            let (sep, entry) = index_entry_at(index_block, block_idx);
            let (k0, _, _) = block.decode_restart_entry(0)?;
            validate_point_ikey(k0)?;
            if k0 != entry.first_key || compare_internal_key(k0, sep).is_gt() {
                return Err(Error::corruption(
                    "SST data block disagrees with index bounds",
                ));
            }
            if ordered {
                let (kl, _, _) = block.decode_restart_entry(block.num_restarts() - 1)?;
                if compare_internal_key(kl, sep).is_gt() {
                    return Err(Error::corruption("SST data block exceeds index separator"));
                }
                self.block_sep_off = 0;
                self.block_sep_len = 0;
            } else {
                self.block_sep_off =
                    (sep.as_ptr() as usize - index_block.data().as_ptr() as usize) as u32;
                self.block_sep_len = sep.len() as u32;
            }
        } else {
            self.block_sep_off = 0;
            self.block_sep_len = 0;
        }
        self.block_data_end = block.data_end_offset() as u32;
        self.block_cursor_offset = 0;
        self.block_next_restart = 1;
        self.block_seg_end = if block.num_restarts() > 1 {
            block.restart_point(1)
        } else {
            self.block_data_end
        };
        self.block_cursor_key.clear();
        self.current_block = Some(block);
        // Release reverse positioning after crossing into a new forward block.
        self.backward_block = None;
        self.backward_block_index = usize::MAX;
        self.current_restart_index = 0;
        self.backward_entries.clear();
        self.backward_keys.clear();
        self.last_value_loc = (false, 0, 0);
        self.block_pos = 0;
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn latch_decode_error(&mut self, err: Error) -> bool {
        self.reset_positioning_state();
        self.err = Some(err.context("block entry decode error"));
        false
    }

    #[cold]
    #[inline(never)]
    fn latch_sep_exceeded(&mut self) -> bool {
        self.reset_positioning_state();
        self.err = Some(Error::corruption("SST data block exceeds index separator"));
        false
    }

    fn reset_positioning_state(&mut self) {
        self.current_block = None;
        self.block_cursor_offset = 0;
        self.block_data_end = 0;
        self.block_next_restart = 1;
        self.block_seg_end = 0;
        self.block_sep_off = 0;
        self.block_sep_len = 0;
        self.block_cursor_key.clear();
        self.at_first_key_from_index = false;
        self.deferred_index_pos = 0;
        self.backward_entries.clear();
        self.backward_keys.clear();
        self.last_value_loc = (false, 0, 0);
        self.block_pos = 0;
        self.current_restart_index = 0;
        self.backward_block = None;
        self.backward_block_index = usize::MAX;
    }

    fn exhaust_at_upper_bound(&mut self) {
        self.reset_positioning_state();
        self.index_pos = usize::MAX;
    }

    fn block_properties_should_skip(&self, entry: &DecodedIndexEntry<'_>) -> bool {
        if self.config.filters.is_empty() {
            return false;
        }
        self.config.filters.iter().any(|filter| {
            let filter_name = filter.name().as_bytes();
            entry.any_property(|name, data| name == filter_name && filter.should_skip(data))
        })
    }

    fn block_exceeds_upper_bound(&self, first_key: &[u8]) -> bool {
        self.config
            .upper
            .as_ref()
            .is_some_and(|ub| user_key(first_key) >= ub.as_slice())
    }

    /// Materialize the deferred block: load the data block for deferred_index_pos.
    fn materialize_deferred_block(&mut self) {
        let block_idx = self.deferred_index_pos as usize;
        if let Some(ref index_block) = self.index_block {
            let (sep, entry) = index_entry_at(index_block, block_idx);
            let res = self
                .reader
                .read_block_cached_block(
                    &entry.handle,
                    Some((entry.first_key, sep)),
                    self.fill_cache,
                )
                .and_then(|(block, ordered)| self.set_block_for_cursor(block, block_idx, ordered));
            if let Err(e) = res {
                self.reset_positioning_state();
                self.err = Some(e.context("block read error"));
            }
        }
    }

    #[inline]
    pub(crate) fn has_current(&self) -> bool {
        self.err.is_none() && self.block_pos < self.backward_entries.len()
    }

    #[cfg(test)]
    #[inline]
    fn backward_entry_owned(&self, pos: usize) -> (Vec<u8>, Vec<u8>) {
        let e = self.backward_entries[pos];
        let key = self.backward_keys[e.key_start..e.key_end].to_vec();
        let val = self
            .backward_block
            .as_ref()
            .map(|b| b.data()[e.val_offset as usize..(e.val_offset + e.val_len) as usize].to_vec())
            .unwrap_or_default();
        (key, val)
    }

    #[inline]
    fn copy_backward_into_lazy(&self, pos: usize, key_buf: &mut Vec<u8>, val_out: &mut LazyValue) {
        let e = self.backward_entries[pos];
        key_buf.clear();
        key_buf.extend_from_slice(&self.backward_keys[e.key_start..e.key_end]);
        if let Some(ref block) = self.backward_block {
            val_out.set_block_ref(block.data_arc(), e.val_offset, e.val_len);
        } else {
            val_out.set_empty();
        }
    }

    /// Seek to the first entry >= target using the index block for O(log N) lookup.
    pub fn seek(&mut self, target: &[u8]) {
        if self.err.is_some() {
            self.reset_positioning_state();
            return;
        }
        if self
            .config
            .upper
            .as_ref()
            .is_some_and(|ub| user_key(target) >= ub.as_slice())
        {
            self.exhaust_at_upper_bound();
            return;
        }
        self.select_partition(self.seek_partition(target));
        self.reset_positioning_state();
        if self.err.is_some() {
            return;
        }
        let Some(index_block) = self.index_block.clone() else {
            self.index_pos = 0;
            return;
        };
        let index_len = index_block.num_restarts() as usize;

        // Quick check: if target > partition's largest key, mark exhausted.
        if index_len > 0 {
            let (last_sep, _) = index_entry_at(&index_block, index_len - 1);
            if compare_internal_key(target, last_sep) == Ordering::Greater {
                self.index_pos = index_len;
                return;
            }
        }

        // Binary search index leaf restarts in place (`0` allocations).
        let idx = TableReader::partition_point_index_leaf(&index_block, target) as usize;

        self.index_pos = idx;

        // Load the found block and seek within it using the cursor
        while self.index_pos < index_len {
            let (sep, entry) = index_entry_at(&index_block, self.index_pos);
            if self.block_exceeds_upper_bound(entry.first_key) {
                self.index_pos = usize::MAX;
                return;
            }
            self.index_pos += 1;
            if self.block_properties_should_skip(&entry) {
                continue;
            }

            // Deferred block read: if target <= first_key, position without I/O
            let first_key = entry.first_key;
            if compare_internal_key(target, first_key) != Ordering::Greater {
                self.at_first_key_from_index = true;
                self.deferred_index_pos = (self.index_pos - 1) as u32;
                self.block_cursor_key.clear();
                self.block_cursor_key.extend_from_slice(first_key);
                self.current_block = None;
                return;
            }

            let block_idx = self.index_pos - 1;
            match self.reader.read_data_block_ordered(
                &entry.handle,
                first_key,
                sep,
                self.fill_cache,
            ) {
                Ok(block) => {
                    self.seek_within_block(block, block_idx, target, compare_internal_key);
                }
                Err(e) => {
                    self.err = Some(e.context("block read error in seek"));
                }
            }
            return;
        }
    }

    /// Position the cursor at the first entry >= `target` within `block`.
    /// If the block contains a malformed entry, records the corruption via
    /// `self.err` instead of silently treating it as "not found" or clean EOF.
    fn seek_within_block<F: Fn(&[u8], &[u8]) -> Ordering>(
        &mut self,
        block: Block,
        block_idx: usize,
        target: &[u8],
        compare: F,
    ) {
        if let Err(e) = self.set_block_for_cursor(block.clone(), block_idx, true) {
            self.reset_positioning_state();
            self.err = Some(e.context("block entry decode error in seek"));
            return;
        }
        let data_end = block.data_end_offset();
        let block_data = block.data();
        let num_restarts = block.num_restarts();

        // Binary search on restart points in place (`shared_len == 0` at restarts).
        let mut left = 0u32;
        let mut right = num_restarts;
        while left < right {
            let mid = left + (right - left) / 2;
            match block
                .decode_restart_entry(mid)
                .and_then(|(rk, _, _)| validate_point_ikey(rk).map(|()| rk))
            {
                Ok(rk) => {
                    if compare(rk, target) == Ordering::Less {
                        left = mid + 1;
                    } else {
                        right = mid;
                    }
                }
                Err(e) => {
                    self.reset_positioning_state();
                    self.err = Some(e.context("block entry decode error in seek"));
                    return;
                }
            }
        }

        let (start, seg_end, next_restart) = if left > 0 {
            let s = block.restart_point(left - 1) as usize;
            let e = if left < num_restarts {
                block.restart_point(left) as usize
            } else {
                data_end
            };
            (s, e, left)
        } else {
            let e = if num_restarts > 1 {
                block.restart_point(1) as usize
            } else {
                data_end
            };
            (0, e, 1)
        };
        let seg_data = &block_data[..seg_end];

        // Scan entries in the selected segment, saving key state on stack SmallVec
        // so we can restore the prefix-compressed key buffer with zero heap allocs.
        let index_bounds = self
            .index_block
            .as_ref()
            .map(|ib| index_entry_at(ib, block_idx));
        self.block_cursor_key.clear();
        let mut prev_key_snapshot = SmallVec::<[u8; 128]>::new();
        let mut offset = start;
        while offset < seg_end {
            let entry_offset = offset;
            prev_key_snapshot.clear();
            prev_key_snapshot.extend_from_slice(&self.block_cursor_key);

            match decode_point_entry_monotonic(
                seg_data,
                offset,
                entry_offset != start,
                entry_offset == start,
                &mut self.block_cursor_key,
            ) {
                Ok((_, _, next_off)) => {
                    if let Some((sep, ref entry)) = index_bounds {
                        if entry_offset == start
                            && start > 0
                            && !compare_internal_key(entry.first_key, &self.block_cursor_key)
                                .is_lt()
                        {
                            self.reset_positioning_state();
                            self.err = Some(Error::corruption(
                                "SST data block disagrees with index bounds",
                            ));
                            return;
                        }
                        if (next_off == data_end
                            || compare(&self.block_cursor_key, target) != Ordering::Less)
                            && compare_internal_key(&self.block_cursor_key, sep).is_gt()
                        {
                            self.reset_positioning_state();
                            self.err =
                                Some(Error::corruption("SST data block exceeds index separator"));
                            return;
                        }
                    }
                    if compare(&self.block_cursor_key, target) != Ordering::Less {
                        self.block_cursor_key.clear();
                        self.block_cursor_key.extend_from_slice(&prev_key_snapshot);
                        self.block_cursor_offset = entry_offset as u32;
                        self.block_data_end = data_end as u32;
                        self.block_next_restart = next_restart;
                        self.block_seg_end = seg_end as u32;
                        self.current_block = Some(block);
                        return;
                    }
                    offset = next_off;
                }
                Err(e) => {
                    self.reset_positioning_state();
                    self.err = Some(e.context("block entry decode error in seek"));
                    return;
                }
            }
        }

        if left < num_restarts {
            match block.decode_restart_entry(left).and_then(|(rk, _, _)| {
                validate_point_ikey(rk)?;
                if !self.block_cursor_key.is_empty()
                    && !compare_internal_key(&self.block_cursor_key, rk).is_lt()
                {
                    return Err(Error::corruption("unordered point keys in SST data block"));
                }
                if let Some((sep, ref entry)) = index_bounds {
                    if left > 0 && !compare_internal_key(entry.first_key, rk).is_lt() {
                        return Err(Error::corruption(
                            "SST data block disagrees with index bounds",
                        ));
                    }
                    if compare_internal_key(rk, sep).is_gt() {
                        return Err(Error::corruption("SST data block exceeds index separator"));
                    }
                }
                Ok(())
            }) {
                Ok(()) => {
                    self.block_cursor_offset = block.restart_point(left);
                    self.block_data_end = data_end as u32;
                    self.block_next_restart = left + 1;
                    self.block_seg_end = if left + 1 < num_restarts {
                        block.restart_point(left + 1)
                    } else {
                        data_end as u32
                    };
                    self.block_cursor_key.clear();
                    self.current_block = Some(block);
                    return;
                }
                Err(e) => {
                    self.reset_positioning_state();
                    self.err = Some(e.context("block entry decode error in seek"));
                    return;
                }
            }
        }

        // All entries < target
        self.block_cursor_offset = data_end as u32;
        self.block_data_end = data_end as u32;
        self.block_next_restart = num_restarts;
        self.block_seg_end = data_end as u32;
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
        if self.err.is_some() {
            self.reset_positioning_state();
            return;
        }
        let clamped_target;
        let target = if let Some(ub) = self.config.upper.as_deref()
            && user_key(target) >= ub
        {
            clamped_target = InternalKey::new(ub, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion);
            clamped_target.as_bytes()
        } else {
            target
        };
        self.select_partition(
            self.seek_partition(target)
                .min(self.reader.partitions.len().saturating_sub(1)),
        );
        self.reset_positioning_state();
        if self.err.is_some() {
            return;
        }
        while let Some(index_block) = self.index_block.clone() {
            let index_len = index_block.num_restarts() as usize;
            let mut left = 0usize;
            let mut right = index_len;
            while left < right {
                let mid = left + (right - left) / 2;
                let (sep, _) = index_entry_at(&index_block, mid);
                if compare_internal_key(sep, target) == Ordering::Less
                    && self
                        .config
                        .upper
                        .as_ref()
                        .is_none_or(|ub| user_key(sep) < ub.as_slice())
                {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            let end = (left + 1).min(index_len);
            for block_index in (0..end).rev() {
                let (sep, entry) = index_entry_at(&index_block, block_index);
                if self.block_exceeds_upper_bound(entry.first_key)
                    || compare_internal_key(entry.first_key, target) == Ordering::Greater
                {
                    continue;
                }
                if self.block_properties_should_skip(&entry) {
                    continue;
                }
                let positioned = self
                    .reader
                    .read_data_block_ordered(&entry.handle, entry.first_key, sep, self.fill_cache)
                    .and_then(|block| self.position_reverse_segment(block, block_index, target));
                match positioned {
                    Ok(true) => return,
                    Ok(false) => continue,
                    Err(error) => {
                        self.err = Some(error.context("block read error in seek_for_prev"));
                        break;
                    }
                }
            }
            if self.err.is_some() || self.partition == 0 {
                break;
            }
            self.select_partition((self.partition - 1) as usize);
        }
        if self.err.is_some() {
            self.index_pos = usize::MAX;
        } else {
            self.index_pos = 0;
        }
    }

    fn decode_backward_point_segment(
        &mut self,
        block: &Block,
        block_index: usize,
        restart: u32,
        lower_exclusive: Option<&[u8]>,
        upper_exclusive: Option<&[u8]>,
    ) -> Result<()> {
        if block.data_end_offset() == 0 {
            return Err(Error::corruption("empty SST data block"));
        }
        block.decode_restart_segment_into(
            restart,
            &mut self.backward_keys,
            &mut self.backward_entries,
        )?;
        if self.backward_entries.is_empty() {
            return Err(Error::corruption("empty SST restart segment"));
        }
        let keys = &self.backward_keys;
        for (i, e) in self.backward_entries.iter().enumerate() {
            let k = &keys[e.key_start..e.key_end];
            validate_point_ikey(k)?;
            if i > 0 {
                let prev = self.backward_entries[i - 1];
                if !compare_internal_key(&keys[prev.key_start..prev.key_end], k).is_lt() {
                    return Err(Error::corruption("unordered point keys in SST data block"));
                }
            }
        }
        let first_e = self.backward_entries[0];
        let first_k = &keys[first_e.key_start..first_e.key_end];
        let last_e = *self.backward_entries.last().unwrap();
        let last_k = &keys[last_e.key_start..last_e.key_end];
        if let Some(lo) = lower_exclusive
            && !compare_internal_key(lo, first_k).is_lt()
        {
            return Err(Error::corruption("unordered point keys in SST data block"));
        }
        if let Some(hi) = upper_exclusive
            && !compare_internal_key(last_k, hi).is_lt()
        {
            return Err(Error::corruption("unordered point keys in SST data block"));
        }
        if let Some(ref index_block) = self.index_block {
            let (sep, entry) = index_entry_at(index_block, block_index);
            let first_ok = if restart == 0 {
                first_k == entry.first_key
            } else {
                compare_internal_key(entry.first_key, first_k).is_lt()
            };
            if !first_ok {
                return Err(Error::corruption(
                    "SST data block disagrees with index bounds",
                ));
            }
            let sep_ok = if restart + 1 == block.num_restarts() {
                compare_internal_key(last_k, sep).is_le()
            } else {
                compare_internal_key(last_k, sep).is_lt()
            };
            if !sep_ok {
                return Err(Error::corruption("SST data block exceeds index separator"));
            }
        }
        Ok(())
    }

    fn position_reverse_segment(
        &mut self,
        block: Block,
        block_index: usize,
        target: &[u8],
    ) -> Result<bool> {
        if block.data_end_offset() == 0 {
            return Err(Error::corruption("empty SST data block"));
        }
        let cmp_bounded = |ub: Option<&[u8]>, key: &[u8], target: &[u8]| -> Ordering {
            if ub.is_some_and(|u| user_key(key) >= u) {
                Ordering::Greater
            } else {
                compare_internal_key(key, target)
            }
        };
        let ub = self.config.upper.as_deref();
        let (mut left, mut right) = (0u32, block.num_restarts());
        while left < right {
            let mid = left + (right - left) / 2;
            let (rk, _, _) = block.decode_restart_entry(mid)?;
            validate_point_ikey(rk)?;
            if cmp_bounded(ub, rk, target) == Ordering::Greater {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        let Some(restart) = left.checked_sub(1) else {
            return Ok(false);
        };
        self.decode_backward_point_segment(&block, block_index, restart, None, None)?;
        let ub = self.config.upper.as_deref();
        let keys = &self.backward_keys;
        let end = self.backward_entries.partition_point(|e| {
            cmp_bounded(ub, &keys[e.key_start..e.key_end], target) != Ordering::Greater
        });
        let Some(position) = end.checked_sub(1) else {
            self.backward_entries.clear();
            self.backward_keys.clear();
            return Ok(false);
        };
        self.index_pos = block_index + 1;
        self.block_pos = position;
        self.current_restart_index = restart;
        self.backward_block = Some(block);
        self.backward_block_index = block_index;
        Ok(true)
    }

    fn prev_step(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        // If we have a previous entry in the current segment, just decrement
        if self.block_pos > 0 {
            self.block_pos -= 1;
            return true;
        }

        // Try loading the previous restart segment within the same block
        if self.current_restart_index > 0
            && let Some(block) = self.backward_block.clone()
        {
            let cur_restart = self.current_restart_index;
            let next_first = match block.decode_restart_entry(cur_restart) {
                Ok((rk, _, _)) => rk,
                Err(e) => {
                    self.reset_positioning_state();
                    self.err = Some(e.context("block entry decode error in prev"));
                    return false;
                }
            };
            self.current_restart_index -= 1;
            let block_index = self.backward_block_index;
            let restart = self.current_restart_index;
            match self.decode_backward_point_segment(
                &block,
                block_index,
                restart,
                None,
                Some(next_first),
            ) {
                Ok(()) => {
                    self.block_pos = self.backward_entries.len() - 1;
                    return true;
                }
                Err(e) => {
                    self.reset_positioning_state();
                    self.err = Some(e.context("block entry decode error in prev"));
                    return false;
                }
            }
        }

        // Need to load the previous block's last segment.
        if self.backward_block_index == usize::MAX {
            return false;
        }
        self.previous_block_end_step(self.backward_block_index)
    }

    /// Move to the previous entry. Returns the entry at the new position,
    /// or None if we've moved before the first entry.
    ///
    /// Uses windowed segment decoding: when crossing restart segment boundaries,
    /// only loads the previous segment (not the entire block).
    #[cfg(test)]
    pub fn prev(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.prev_step()
            .then(|| self.backward_entry_owned(self.block_pos))
    }

    /// Walk backwards from an exclusive leaf-local block position, crossing
    /// index partitions only when all earlier blocks in this leaf are spent.
    fn previous_block_end_step(&mut self, mut before: usize) -> bool {
        loop {
            while before == 0 {
                if self.partition == 0 {
                    self.reset_positioning_state();
                    self.index_pos = 0;
                    return false;
                }
                self.select_partition((self.partition - 1) as usize);
                if self.err.is_some() {
                    return false;
                }
                before = self.index_len();
            }
            before -= 1;
            let prev_block_index = before;
            let Some(index_block) = self.index_block.as_ref() else {
                return false;
            };
            let (sep, entry) = index_entry_at(index_block, prev_block_index);
            if self.block_properties_should_skip(&entry) {
                continue;
            }
            let handle = entry.handle;
            match self.reader.read_block_cached_block(
                &handle,
                Some((entry.first_key, sep)),
                self.fill_cache,
            ) {
                Ok((block, _)) => {
                    let last_restart = block.num_restarts().saturating_sub(1);
                    if let Err(e) = self.decode_backward_point_segment(
                        &block,
                        prev_block_index,
                        last_restart,
                        None,
                        None,
                    ) {
                        self.reset_positioning_state();
                        self.err = Some(e.context("block entry decode error in prev"));
                        return false;
                    }
                    self.block_pos = self.backward_entries.len() - 1;
                    self.index_pos = prev_block_index + 1;
                    self.current_restart_index = last_restart;
                    self.backward_block = Some(block);
                    self.backward_block_index = prev_block_index;
                    return true;
                }
                Err(e) => {
                    self.reset_positioning_state();
                    self.err = Some(e.context("block read error in prev"));
                }
            }

            return false;
        }
    }

    /// Return the current entry without advancing, or None if not positioned.
    #[cfg(test)]
    pub fn current(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.has_current()
            .then(|| self.backward_entry_owned(self.block_pos))
    }

    fn load_next_block(&mut self) -> bool {
        if let Some(block) = self.backward_block.clone()
            && self.current_restart_index + 1 < block.num_restarts()
        {
            let next_restart = self.current_restart_index + 1;
            let check = block
                .decode_restart_entry(next_restart)
                .and_then(|(next_first, _, _)| {
                    validate_point_ikey(next_first)?;
                    if let Some(e) = self.backward_entries.last() {
                        let prev_last = &self.backward_keys[e.key_start..e.key_end];
                        if !compare_internal_key(prev_last, next_first).is_lt() {
                            return Err(Error::corruption(
                                "unordered point keys in SST data block",
                            ));
                        }
                    }
                    Ok(())
                });
            if let Err(error) = check {
                self.reset_positioning_state();
                self.err = Some(error.context("block entry decode error"));
                return false;
            }
            self.current_restart_index = next_restart;
            let block_index = self.backward_block_index;
            match self.decode_backward_point_segment(&block, block_index, next_restart, None, None)
            {
                Ok(()) => {
                    self.block_pos = 0;
                    return true;
                }
                Err(error) => {
                    self.reset_positioning_state();
                    self.err = Some(error.context("block entry decode error"));
                    return false;
                }
            }
        }
        if self.err.is_some() || self.index_pos == usize::MAX {
            return false;
        }
        self.ensure_index();
        loop {
            if self.err.is_some() || self.index_pos == usize::MAX {
                return false;
            }
            let Some(index_block) = self.index_block.clone() else {
                return false;
            };
            let index_len = index_block.num_restarts() as usize;
            while self.index_pos < index_len {
                let block_idx = self.index_pos;

                let (sep, entry) = index_entry_at(&index_block, block_idx);

                // Skip blocks whose first_key user key >= upper_bound
                if self.block_exceeds_upper_bound(entry.first_key) {
                    self.index_pos = usize::MAX;
                    return false;
                }

                // Skip blocks based on block property filters
                if self.block_properties_should_skip(&entry) {
                    self.index_pos += 1;
                    continue;
                }

                let handle = entry.handle;
                let first_key = entry.first_key;
                self.index_pos += 1;

                // Detect sequential access and trigger readahead
                if block_idx == self.prev_block_index.wrapping_add(1) {
                    self.sequential_reads += 1;
                    if self.sequential_reads >= 2 {
                        self.maybe_readahead(&index_block, block_idx);
                    }
                } else {
                    self.sequential_reads = 0;
                }
                self.prev_block_index = block_idx;

                match self
                    .reader
                    .read_block_cached_block(&handle, Some((first_key, sep)), self.fill_cache)
                    .and_then(|(block, ordered)| {
                        self.set_block_for_cursor(block, block_idx, ordered)
                    }) {
                    Ok(()) => return true,
                    Err(e) => {
                        self.reset_positioning_state();
                        self.err =
                            Some(e.context(format!("block read error at index {block_idx}")));
                        return false;
                    }
                }
            }
            let part = self.partition as usize;
            if part + 1 >= self.reader.partitions.len() {
                return false;
            }
            if self
                .config
                .upper
                .as_deref()
                .is_some_and(|ub| self.reader.partitions[part].0.prefix.as_ref() >= ub)
            {
                self.index_pos = usize::MAX;
                return false;
            }
            self.select_partition(part + 1);
        }
    }

    /// Issue a readahead hint for upcoming blocks.
    fn maybe_readahead(&self, index_block: &Block, current_idx: usize) {
        // Prefetch the next N blocks (adaptive: starts at 2, grows to 8)
        let prefetch_count = (self.sequential_reads as usize).min(8);
        let start = current_idx + 1;
        let end = (start + prefetch_count).min(index_block.num_restarts() as usize);
        if start >= end {
            return;
        }

        // Compute the file range covering the upcoming blocks
        let (_, first_entry) = index_entry_at(index_block, start);
        let (_, last_entry) = index_entry_at(index_block, end - 1);
        if let Some((offset, len)) = checked_readahead_range(first_entry.handle, last_entry.handle)
        {
            self.reader.advise_willneed(offset, len);
        }
    }
}

impl Iterator for TableIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        crate::iterator::merge::SeekableIterator::next_into(self, &mut key_buf, &mut val_buf)
            .then_some((key_buf, val_buf))
    }
}

impl crate::iterator::merge::SeekableIterator for TableIterator {
    fn seek_to(&mut self, target: &[u8]) {
        self.seek(target);
    }

    fn current(&self) -> Option<(Vec<u8>, LazyValue)> {
        let mut key_buf = Vec::new();
        let mut val_out = LazyValue::empty();
        self.current_into_lazy(&mut key_buf, &mut val_out)
            .then_some((key_buf, val_out))
    }

    fn current_into_lazy(&self, key_buf: &mut Vec<u8>, val_out: &mut LazyValue) -> bool {
        if !self.has_current() {
            return false;
        }
        self.copy_backward_into_lazy(self.block_pos, key_buf, val_out);
        true
    }

    fn prev(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        let mut key_buf = Vec::new();
        let mut val_out = LazyValue::empty();
        self.prev_into_lazy(&mut key_buf, &mut val_out)
            .then_some((key_buf, val_out))
    }

    fn prev_into_lazy(&mut self, key_buf: &mut Vec<u8>, val_out: &mut LazyValue) -> bool {
        if !self.prev_step() {
            return false;
        }
        self.copy_backward_into_lazy(self.block_pos, key_buf, val_out);
        true
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        TableIterator::seek_for_prev(self, target);
    }

    fn seek_to_first(&mut self) {
        if self.err.is_some() {
            self.reset_positioning_state();
            return;
        }
        // Reset to the beginning of the table
        self.select_partition(0);
        self.reset_positioning_state();
        self.index_pos = 0;
    }

    fn seek_to_last(&mut self) {
        if self.err.is_some() {
            self.reset_positioning_state();
            return;
        }
        if let Some(ub) = self.config.upper.clone() {
            let target = InternalKey::new(&ub, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion);
            self.seek_for_prev(target.as_bytes());
            return;
        }
        self.select_partition(self.reader.partitions.len().saturating_sub(1));
        self.reset_positioning_state();
        if self.err.is_none() {
            let before = self.index_len();
            self.previous_block_end_step(before);
        }
    }

    #[inline]
    fn advance_key(&mut self, key_buf: &mut Vec<u8>) -> bool {
        // F7: bail immediately if a prior I/O error was recorded
        if self.err.is_some() {
            return false;
        }
        // F6: materialize deferred block if positioned at first_key from index
        if self.at_first_key_from_index {
            self.at_first_key_from_index = false;
            self.materialize_deferred_block();
            if self.err.is_some() {
                return false;
            }
        }
        loop {
            // Try cursor-based path (forward iteration)
            if let Some(ref block) = self.current_block
                && self.block_cursor_offset < self.block_data_end
            {
                let mut at_restart = self.block_cursor_offset == 0;
                if self.block_cursor_offset == self.block_seg_end {
                    at_restart = true;
                    let r = self.block_next_restart;
                    self.block_next_restart = r + 1;
                    self.block_seg_end = if r + 1 < block.num_restarts() {
                        block.restart_point(r + 1)
                    } else {
                        self.block_data_end
                    };
                }
                let has_prev = !self.block_cursor_key.is_empty();
                if !has_prev {
                    at_restart = true;
                }
                let entry_data = &block.data()[..self.block_seg_end as usize];
                match decode_point_entry_monotonic(
                    entry_data,
                    self.block_cursor_offset as usize,
                    has_prev,
                    at_restart,
                    &mut self.block_cursor_key,
                ) {
                    Ok((vs, vl, next)) => {
                        if self.block_sep_len > 0
                            && let Some(ref ib) = self.index_block
                        {
                            let s_off = self.block_sep_off as usize;
                            let sep = &ib.data()[s_off..s_off + self.block_sep_len as usize];
                            if compare_internal_key(&self.block_cursor_key, sep).is_gt() {
                                return self.latch_sep_exceeded();
                            }
                        }
                        if let Some(ref ub) = self.config.upper
                            && user_key(&self.block_cursor_key) >= ub.as_slice()
                        {
                            self.exhaust_at_upper_bound();
                            return false;
                        }
                        self.block_cursor_offset = next as u32;
                        key_buf.clear();
                        key_buf.extend_from_slice(&self.block_cursor_key);
                        self.last_value_loc = (false, vs as u32, vl as u32);
                        return true;
                    }
                    Err(e) => return self.latch_decode_error(e),
                }
            }
            // Try materialized entries (backward iteration)
            if self.block_pos < self.backward_entries.len() {
                let e = self.backward_entries[self.block_pos];
                let k = &self.backward_keys[e.key_start..e.key_end];
                if let Some(ref ub) = self.config.upper
                    && user_key(k) >= ub.as_slice()
                {
                    self.exhaust_at_upper_bound();
                    return false;
                }
                key_buf.clear();
                key_buf.extend_from_slice(k);
                self.last_value_loc = (true, e.val_offset, e.val_len);
                self.block_pos += 1;
                return true;
            }
            if !self.load_next_block() {
                return false;
            }
        }
    }

    fn materialize_value_into(&self, val_out: &mut LazyValue) {
        let (from_backward, offset, len) = self.last_value_loc;
        let block = if from_backward {
            self.backward_block.as_ref()
        } else {
            self.current_block.as_ref()
        };
        if let Some(b) = block {
            val_out.set_block_ref(b.data_arc(), offset, len);
        } else {
            val_out.set_empty();
        }
    }

    fn next_into(&mut self, key_buf: &mut Vec<u8>, value_buf: &mut Vec<u8>) -> bool {
        if !self.advance_key(key_buf) {
            return false;
        }
        let (from_backward, offset, len) = self.last_value_loc;
        let block = if from_backward {
            self.backward_block.as_ref()
        } else {
            self.current_block.as_ref()
        };
        value_buf.clear();
        if let Some(b) = block {
            value_buf.extend_from_slice(&b.data()[offset as usize..(offset + len) as usize]);
        }
        true
    }

    fn prefetch_first_block(&mut self) {
        self.ensure_index();
        if let Some(index) = self.index_block.as_ref()
            && index.num_restarts() > 0
        {
            let (_, entry) = index_entry_at(index, 0);
            if let Some((offset, len)) = checked_readahead_range(entry.handle, entry.handle) {
                self.reader.advise_willneed(offset, len);
            }
        }
    }

    fn set_config(&mut self, config: Arc<IterConfig>) {
        self.config = config;
    }

    fn iter_error(&self) -> Option<&Error> {
        self.err.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
    };

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
            builder.add_point(key.as_bytes(), val.as_bytes()).unwrap();
        }
        builder.finish().unwrap();
        path
    }

    #[test]
    fn test_checked_readahead_range_rejects_malformed_handles() {
        let first = BlockHandle::new(100, 10);
        let last = BlockHandle::new(120, 20);
        assert_eq!(checked_readahead_range(first, last), Some((100, 45)));

        let reversed = BlockHandle::new(90, 20);
        assert_eq!(checked_readahead_range(first, reversed), None);

        let overflowing = BlockHandle::new(120, u64::MAX);
        assert_eq!(checked_readahead_range(first, overflowing), None);
    }

    #[test]
    fn test_table_read_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 100);

        let reader = TableReader::open(&path).unwrap();
        let entries = reader.iter().unwrap();
        assert_eq!(entries.len(), 100);

        for (i, (k, v)) in entries.iter().enumerate() {
            assert_eq!(user_key(k), format!("key_{:06}", i).as_bytes());
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
                user_key(&k),
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
    fn reverse_seek_copies_only_its_restart_segment() {
        use crate::BlockCache;
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seek.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1 << 20,
                block_restart_interval: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let key = |i| InternalKey::new(&[i], 1, ValueType::Value).into_bytes();
        builder.add(&key(0), b"v").unwrap();
        for i in 1..16 {
            builder.add(&key(i), &[7; 8192]).unwrap();
        }
        builder.finish().unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        let reader = TableReader::open_with_all(&path, 1, Some(cache), None).unwrap();
        let mut iter = TableIterator::new(Arc::new(reader));
        let k0 = key(0);
        iter.seek_for_prev(&k0); // warm the index and data block
        let (_, allocated) = crate::test_alloc::count_allocated(|| iter.seek_for_prev(&k0));
        assert_eq!(
            allocated, 0,
            "warm reverse seek allocated {allocated} bytes"
        );
        assert_eq!(iter.backward_entries.len(), 1);
        for i in 0..16 {
            assert_eq!(iter.next().unwrap().0, key(i));
        }
        assert!(iter.next().is_none());
        assert!(iter.err.is_none());
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
    struct SkipPrefixCollector {
        has_matching_key: bool,
    }

    impl BlockPropertyCollector for SkipPrefixCollector {
        fn add(&mut self, key: &[u8]) {
            self.has_matching_key |= !key.starts_with(b"a_skip_");
        }

        fn finish_block(&mut self) -> Vec<u8> {
            if std::mem::take(&mut self.has_matching_key) {
                b"keep".to_vec()
            } else {
                b"skip".to_vec()
            }
        }

        fn name(&self) -> &str {
            "first-prefix"
        }
    }

    struct SkipBlocksFilter;

    impl BlockPropertyFilter for SkipBlocksFilter {
        fn key_matches(&self, key: &[u8]) -> bool {
            !key.starts_with(b"a_skip_")
        }
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
                block_property_collectors: vec![Box::<SkipPrefixCollector>::default()],
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

    /// Regression test for the deferred-block-read error-swallowing bug:
    /// `seek()` can position the iterator at a block's `first_key` without
    /// reading the block (`at_first_key_from_index = true`), deferring the
    /// actual I/O to `materialize_deferred_block()` on the first subsequent
    /// `next()`. If that deferred read fails, `next()` must surface the
    /// error immediately rather than silently falling through to the next
    /// (readable) block as if the scan were undisturbed.
    #[test]
    fn test_deferred_block_read_error_does_not_skip_to_next_block() {
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};

        use crate::iterator::merge::SeekableIterator;
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deferred_err.sst");

        // block_size: 1 forces exactly one entry per block, so each block's
        // first_key equals its only key and seeking to that key deterministically
        // triggers the deferred-read fast path in `seek()`.
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        const COUNT: usize = 6;
        let keys: Vec<Vec<u8>> = (0..COUNT)
            .map(|i| {
                let user_key = format!("key_{:06}", i);
                InternalKey::new(user_key.as_bytes(), (COUNT - i) as u64, ValueType::Value)
                    .into_bytes()
            })
            .collect();
        for (i, k) in keys.iter().enumerate() {
            builder.add(k, format!("value_{i}").as_bytes()).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        let entries = reader.cached_index_entries().unwrap();
        assert_eq!(entries.len(), COUNT, "expected one block per entry");

        // Corrupt only the stored CRC byte of block index 2's trailer, in place,
        // without changing the file's length or touching any other block's bytes.
        // Blocks 0 and 1 (before it) and 3..5 (after it) stay byte-for-byte intact,
        // so block 3 remains genuinely readable — if the deferred error were
        // silently swallowed, `next()` would incorrectly yield block 3's entry
        // instead of surfacing the corruption.
        let target = &entries[2];
        let crc_byte_offset = target.handle.offset + target.handle.size + 1;
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.seek(SeekFrom::Start(crc_byte_offset)).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            f.seek(SeekFrom::Start(crc_byte_offset)).unwrap();
            f.write_all(&[byte[0].wrapping_add(1)]).unwrap();
        }

        let mut iter = TableIterator::new(Arc::new(reader));
        iter.seek(&keys[2]);

        let result = iter.next();
        assert!(
            result.is_none(),
            "next() must return None on a deferred block read error instead of \
             silently yielding an entry from the next block, got {:?}",
            result
        );
        let err = iter.iter_error();
        assert!(
            err.is_some_and(|e| e.to_string().contains("CRC")),
            "iter_error() must report the deferred block CRC mismatch, got {:?}",
            err
        );
    }

    /// Corrupt one byte inside the block at `handle` (relative offset
    /// `rel_off`), then recompute the trailer CRC over the corrupted bytes
    /// so the whole-block CRC check passes and the mid-block entry-decode
    /// path is what must catch it.
    fn corrupt_block_byte_recompute_crc(
        path: &Path,
        handle_offset: u64,
        handle_size: u64,
        rel_off: usize,
    ) {
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut block = vec![0u8; handle_size as usize];
        f.seek(SeekFrom::Start(handle_offset)).unwrap();
        f.read_exact(&mut block).unwrap();
        let mut type_byte = [0u8; 1];
        f.read_exact(&mut type_byte).unwrap();

        // 0xFF sets the varint continuation bit → shared-prefix length
        // >= 127, always exceeding the previous key → decode error.
        block[rel_off] = 0xFF;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&block);
        hasher.update(&type_byte);
        let crc = hasher.finalize();

        f.seek(SeekFrom::Start(handle_offset)).unwrap();
        f.write_all(&block).unwrap();
        f.seek(SeekFrom::Current(1)).unwrap(); // skip type byte
        f.write_all(&crc.to_le_bytes()).unwrap();
    }

    /// Read a block's restart array from the raw file and return the byte
    /// offset (relative to the block start) of restart point `idx`.
    fn restart_entry_offset(
        path: &Path,
        handle_offset: u64,
        handle_size: u64,
        idx: usize,
    ) -> usize {
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom};

        let mut f = OpenOptions::new().read(true).open(path).unwrap();
        let mut block = vec![0u8; handle_size as usize];
        f.seek(SeekFrom::Start(handle_offset)).unwrap();
        f.read_exact(&mut block).unwrap();

        let n = u32::from_le_bytes(block[block.len() - 4..].try_into().unwrap()) as usize;
        assert!(idx < n, "restart index {idx} out of range ({n} restarts)");
        let restarts_start = block.len() - 4 - 4 * n;
        u32::from_le_bytes(
            block[restarts_start + 4 * idx..restarts_start + 4 * idx + 4]
                .try_into()
                .unwrap(),
        ) as usize
    }

    /// Regression test for the missed-caller gap in the corruption-vs-EOF
    /// fix: `parse_index_entries` iterated the index block without checking
    /// `BlockIterator::error()`, so a CRC-passing corrupt index entry
    /// silently truncated the cached entry list — every data block past the
    /// corruption point vanished from scans and seeks (and from compaction
    /// inputs) with `iter_error() == None`, while point lookups through
    /// `seek_by` errored loudly on the same corruption.
    #[test]
    fn test_corrupt_index_entry_is_error_not_silent_truncation() {
        use crate::iterator::merge::SeekableIterator;
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt_index.sst");

        // block_size: 1 → one entry per data block → 6 index entries;
        // block_restart_interval: 1 → every index entry is a restart point,
        // so the index block's restart array gives exact entry offsets.
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1,
                block_restart_interval: 1,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        const COUNT: usize = 6;
        let keys: Vec<Vec<u8>> = (0..COUNT)
            .map(|i| {
                let user_key = format!("key_{:06}", i);
                InternalKey::new(user_key.as_bytes(), (COUNT - i) as u64, ValueType::Value)
                    .into_bytes()
            })
            .collect();
        for (i, k) in keys.iter().enumerate() {
            builder.add(k, format!("value_{i}").as_bytes()).unwrap();
        }
        builder.finish().unwrap();

        let index_handle = TableReader::open(&path).unwrap().partitions[0].1;
        let (index_offset, index_size) = (index_handle.offset, index_handle.size);

        // Corrupt index entry #3 of 6 (mid-block) and re-seal the CRC.
        let entry3 = restart_entry_offset(&path, index_offset, index_size, 3);
        corrupt_block_byte_recompute_crc(&path, index_offset, index_size, entry3);

        let reader = TableReader::open(&path).unwrap();

        // The cached index parse must fail loudly...
        assert!(
            reader.cached_index_entries().is_err(),
            "cached_index_entries() must propagate a corrupt index entry, \
             not return a silently truncated list"
        );

        // ...and a production scan must surface the error rather than
        // silently yielding only the first 3 of 6 entries.
        let mut iter = TableIterator::new(Arc::new(reader));
        iter.seek_to_first();
        let mut yielded = 0;
        while iter.next().is_some() {
            yielded += 1;
        }
        assert!(
            iter.iter_error().is_some(),
            "scan over a corrupt index must set iter_error(), got {} entries \
             with no error",
            yielded
        );
    }

    /// Mid-block entry decode failure must stop `Iterator::next` before loading
    /// later blocks. Falling through to `load_next_block` after setting `err`
    /// used to silently yield post-corruption keys (and, via boxed compaction
    /// sources that ignore `iter_error`, install truncated outputs).
    #[test]
    fn test_mid_block_entry_decode_error_stops_iterator_next() {
        use crate::iterator::merge::{IterSource, MergingIterator, SeekableIterator};
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mid_block_corrupt.sst");

        // Small blocks + restart every entry so we can corrupt entry #1 of a
        // multi-entry early block and still have later blocks intact.
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 64,
                block_restart_interval: 1,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        const COUNT: usize = 40;
        for i in 0..COUNT {
            let ik = InternalKey::new(
                format!("key_{:06}", i).as_bytes(),
                (COUNT - i) as u64,
                ValueType::Value,
            );
            builder
                .add(ik.as_bytes(), format!("value_{i}").as_bytes())
                .unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        let entries = reader.cached_index_entries().unwrap();
        assert!(
            entries.len() >= 2,
            "fixture needs multiple data blocks, got {}",
            entries.len()
        );

        // Corrupt entry #1 (second restart) inside the first data block and
        // re-seal the CRC so only entry decode (not whole-block CRC) fails.
        let first = &entries[0];
        let rel = restart_entry_offset(&path, first.handle.offset, first.handle.size, 1);
        corrupt_block_byte_recompute_crc(&path, first.handle.offset, first.handle.size, rel);

        let reader = Arc::new(TableReader::open(&path).unwrap());
        let mut iter = TableIterator::new(Arc::clone(&reader));
        iter.seek_to_first();
        let mut yielded = 0usize;
        while iter.next().is_some() {
            yielded += 1;
            assert!(
                yielded < COUNT,
                "Iterator::next must not scan past a mid-block decode error"
            );
        }
        assert!(
            iter.iter_error().is_some(),
            "mid-block decode must set iter_error after stop, yielded {yielded}"
        );
        // First restart entry can still be produced before the corrupt one.
        assert!(yielded < COUNT, "must not yield every key in the file");

        // Compaction-shaped path: from_table_iter must surface the same error
        // through MergingIterator::error (from_boxed cannot).
        let mut mi = MergingIterator::new(
            vec![IterSource::from_table_iter(
                TableIterator::new(reader).with_fill_cache(false),
            )],
            crate::types::compare_internal_key,
        );
        let mut merge_yielded = 0usize;
        while mi.next_entry().is_some() {
            merge_yielded += 1;
        }
        assert!(
            mi.error().is_some(),
            "merging from_table_iter must report mid-block decode via error(), \
             yielded {merge_yielded}"
        );
    }

    /// Regression test for the metaindex sibling of the same gap:
    /// `read_metaindex` iterated without checking `BlockIterator::error()`.
    /// Metaindex keys sort `filter.*` before `rangedelblock`, so a corrupt
    /// earlier entry silently dropped `range_del_handle`,
    /// permanently resurrecting range-deleted data. Open must fail instead.
    #[test]
    fn test_corrupt_metaindex_entry_fails_open_not_silent_tombstone_loss() {
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom};

        use crate::sst::format::{FOOTER_SIZE, decode_footer};
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt_metaindex.sst");

        // bloom on + range tombstone → metaindex holds `filter.bloom`
        // followed by `rangedelblock`.
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_restart_interval: 1,
                bloom_bits_per_key: 10,
                ..Default::default()
            },
        )
        .unwrap();
        builder
            .add(
                InternalKey::new(b"aaa", 10, ValueType::Value).as_bytes(),
                b"val_a",
            )
            .unwrap();
        builder
            .add(
                InternalKey::new(b"bbb", 9, ValueType::RangeDeletion).as_bytes(),
                b"ccc",
            )
            .unwrap();
        builder.finish().unwrap();

        // Sanity: intact file opens and holds exactly one range tombstone.
        {
            let reader = TableReader::open(&path).unwrap();
            let tombstones = reader.get_range_tombstones().unwrap();
            assert_eq!(tombstones.len(), 1, "fixture must carry a range tombstone");
        }

        // Locate the metaindex block via the footer and corrupt its first
        // entry (`filter.bloom` — sorted before `rangedelblock`).
        let (meta_offset, meta_size) = {
            let mut f = OpenOptions::new().read(true).open(&path).unwrap();
            let file_len = f.metadata().unwrap().len();
            f.seek(SeekFrom::Start(file_len - FOOTER_SIZE as u64))
                .unwrap();
            let mut footer = [0u8; FOOTER_SIZE];
            f.read_exact(&mut footer).unwrap();
            let (meta_handle, _index) = decode_footer(&footer).unwrap();
            (meta_handle.offset, meta_handle.size)
        };
        // Entry #1 (second entry) keeps the corruption strictly mid-block.
        let entry1 = restart_entry_offset(&path, meta_offset, meta_size, 1);
        corrupt_block_byte_recompute_crc(&path, meta_offset, meta_size, entry1);

        // Open must fail loudly instead of succeeding with zero tombstones.
        assert!(
            TableReader::open(&path).is_err(),
            "a corrupt metaindex entry must fail open(), not silently drop \
             the range-tombstone block handle"
        );
    }

    /// Regression: a zero metaindex size in the (unchecksummed) footer was
    /// read as "no metadata", so open succeeded without the range-tombstone
    /// block and the file's range deletions stopped hiding older data.
    #[test]
    fn test_zero_metaindex_size_fails_open() {
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write};

        use crate::sst::format::FOOTER_SIZE;
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zero_metaindex.sst");
        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        builder
            .add(
                InternalKey::new(b"aaa", 10, ValueType::Value).as_bytes(),
                b"val_a",
            )
            .unwrap();
        builder
            .add(
                InternalKey::new(b"bbb", 9, ValueType::RangeDeletion).as_bytes(),
                b"ccc",
            )
            .unwrap();
        builder.finish().unwrap();

        // Footer bytes 8..16 hold the metaindex block size.
        let mut f = OpenOptions::new().write(true).open(&path).unwrap();
        let file_len = f.metadata().unwrap().len();
        f.seek(SeekFrom::Start(file_len - FOOTER_SIZE as u64 + 8))
            .unwrap();
        f.write_all(&[0u8; 8]).unwrap();
        drop(f);

        let err = match TableReader::open(&path) {
            Ok(reader) => panic!(
                "open must fail, got {} tombstones",
                reader.get_range_tombstones().unwrap().len()
            ),
            Err(err) => err,
        };
        assert_eq!(err.kind(), crate::ErrorKind::Corruption);
    }

    #[test]
    fn test_latched_error_stops_prev_current_and_reseeks() {
        use crate::iterator::merge::SeekableIterator;
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prev_error_latch.sst");

        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 4096,
                block_restart_interval: 1,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..3usize {
            let ik = InternalKey::new(
                format!("key_{i:04}").as_bytes(),
                (10 - i) as u64,
                ValueType::Value,
            );
            builder
                .add(ik.as_bytes(), format!("val_{i}").as_bytes())
                .unwrap();
        }
        builder.finish().unwrap();

        let entries = TableReader::open(&path)
            .unwrap()
            .cached_index_entries()
            .unwrap();
        assert_eq!(entries.len(), 1, "all 3 entries must share one data block");
        let block_handle = entries[0].handle;
        let rel = restart_entry_offset(&path, block_handle.offset, block_handle.size, 1);
        corrupt_block_byte_recompute_crc(&path, block_handle.offset, block_handle.size, rel);

        let reader = Arc::new(TableReader::open(&path).unwrap());
        let mut iter = TableIterator::new(reader);
        SeekableIterator::seek_to_last(&mut iter);
        assert!(iter.current().is_some(), "segment 2 decodes cleanly");

        // Stepping backward into segment 1 hits the damaged entry and latches err.
        assert!(iter.prev().is_none());
        assert!(iter.iter_error().is_some());
        assert!(
            iter.current().is_none(),
            "current() must clear once prev() latches an error"
        );
        assert!(
            iter.prev().is_none(),
            "a second prev() must not skip past segment 1 and yield segment 0"
        );

        let target0 = InternalKey::new(b"key_0000", 10, ValueType::Value);
        let target2 = InternalKey::new(b"key_0002", 0, ValueType::Deletion);
        iter.seek(target0.as_bytes());
        assert!(iter.next().is_none());
        iter.seek_for_prev(target2.as_bytes());
        assert!(iter.current().is_none());
        assert!(iter.prev().is_none());
        SeekableIterator::seek_to_first(&mut iter);
        assert!(iter.next().is_none());
        SeekableIterator::seek_to_last(&mut iter);
        assert!(iter.current().is_none());
        assert!(iter.iter_error().is_some());
    }

    #[test]
    fn test_iterator_next_enforces_upper_bound_and_stops_scanning_blocks() {
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};

        use crate::iterator::merge::SeekableIterator;
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upper_bound_next.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 4096,
                block_restart_interval: 16,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for key in [b"k1", b"k2", b"k3", b"k4"] {
            let ik = InternalKey::new(key, 10, ValueType::Value);
            builder.add(ik.as_bytes(), key).unwrap();
        }
        builder.finish().unwrap();

        let reader = Arc::new(TableReader::open(&path).unwrap());
        let mut iter = TableIterator::new(Arc::clone(&reader));
        SeekableIterator::set_bounds(&mut iter, None, Some(b"k3"));
        let seek_k1 = InternalKey::new(b"k1", 0, ValueType::Deletion);
        iter.seek_for_prev(seek_k1.as_bytes());
        let yielded: Vec<Vec<u8>> = (&mut iter)
            .map(|(k, _)| InternalKeyRef::new(&k).user_key().to_vec())
            .collect();
        assert_eq!(
            yielded,
            vec![b"k1".to_vec(), b"k2".to_vec()],
            "Iterator::next after seek_for_prev must stop at upper_bound"
        );

        // Multi-block table: hitting upper_bound in
        // block 0 must exhaust immediately rather than reading block 1.
        let multi_path = dir.path().join("upper_bound_blocks.sst");
        let mut builder = TableBuilder::new(
            &multi_path,
            TableBuildOptions {
                block_size: 1,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for key in [b"a", b"m", b"z"] {
            let ik = InternalKey::new(key, 10, ValueType::Value);
            builder.add(ik.as_bytes(), b"val").unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&multi_path).unwrap();
        let cached = reader.cached_index_entries().unwrap();
        assert_eq!(cached.len(), 3);
        let block2_crc = cached[2].handle.offset + cached[2].handle.size + 1;
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&multi_path)
                .unwrap();
            f.seek(SeekFrom::Start(block2_crc)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Start(block2_crc)).unwrap();
            f.write_all(&[b[0].wrapping_add(1)]).unwrap();
        }

        let reader = Arc::new(TableReader::open(&multi_path).unwrap());
        let mut iter = TableIterator::new(reader);
        SeekableIterator::set_bounds(&mut iter, None, Some(b"m"));
        assert_eq!(
            iter.next()
                .map(|(k, _)| InternalKeyRef::new(&k).user_key().to_vec()),
            Some(b"a".to_vec())
        );
        assert!(iter.next().is_none());
        assert!(
            iter.iter_error().is_none(),
            "reaching upper_bound in block 1 must not read out-of-bound block 2: {:?}",
            iter.iter_error()
        );
    }

    #[test]
    fn upper_bound_prunes_backward_seeks_seek_to_last_and_out_of_bound_forward_seek() {
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};

        use crate::iterator::merge::SeekableIterator;
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upper_bound_reverse.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for key in [b"a", b"m", b"z"] {
            let ik = InternalKey::new(key, 10, ValueType::Value);
            builder.add(ik.as_bytes(), b"val").unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        let cached = reader.cached_index_entries().unwrap();
        assert_eq!(cached.len(), 3);
        let block2_crc = cached[2].handle.offset + cached[2].handle.size + 1;
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.seek(SeekFrom::Start(block2_crc)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Start(block2_crc)).unwrap();
            f.write_all(&[b[0].wrapping_add(1)]).unwrap();
        }

        let reader = Arc::new(TableReader::open(&path).unwrap());
        let mut iter = TableIterator::new(Arc::clone(&reader));
        SeekableIterator::set_bounds(&mut iter, None, Some(b"r"));

        SeekableIterator::seek_to_last(&mut iter);
        assert!(iter.iter_error().is_none());
        assert_eq!(
            iter.current()
                .map(|(k, _)| InternalKeyRef::new(&k).user_key().to_vec()),
            Some(b"m".to_vec())
        );

        let seek_z = InternalKey::new(b"z", 100, ValueType::Value);
        iter.seek_for_prev(seek_z.as_bytes());
        assert!(iter.iter_error().is_none());
        assert_eq!(
            iter.current()
                .map(|(k, _)| InternalKeyRef::new(&k).user_key().to_vec()),
            Some(b"m".to_vec())
        );

        iter.seek(seek_z.as_bytes());
        assert!(iter.next().is_none());
        assert!(iter.iter_error().is_none());
    }

    #[test]
    fn backward_exhaustion_clears_current_and_does_not_wrap_into_tail_blocks() {
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backward_exhaust.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for key in [b"b", b"m", b"z"] {
            let ik = InternalKey::new(key, 10, ValueType::Value);
            builder.add(ik.as_bytes(), b"val").unwrap();
        }
        builder.finish().unwrap();

        let reader = Arc::new(TableReader::open(&path).unwrap());
        let mut iter = TableIterator::new(reader);

        // 1. seek_for_prev before the table's smallest key must stay exhausted on prev().
        let before_first = InternalKey::new(b"a", 100, ValueType::Value);
        iter.seek_for_prev(before_first.as_bytes());
        assert!(iter.current().is_none());
        assert!(
            iter.prev().is_none(),
            "prev() after an unpositioned seek_for_prev must not wrap into tail blocks"
        );
        assert!(iter.current().is_none());

        // 2. Stepping prev() off the first entry must clear current() and reset to start.
        let at_first = InternalKey::new(b"b", 0, ValueType::Deletion);
        iter.seek_for_prev(at_first.as_bytes());
        assert_eq!(
            iter.current().map(|(k, _)| user_key(&k).to_vec()),
            Some(b"b".to_vec())
        );
        assert!(iter.prev().is_none());
        assert!(
            iter.current().is_none(),
            "current() must be None once prev() steps before the first entry"
        );
        assert_eq!(
            iter.next().map(|(k, _)| user_key(&k).to_vec()),
            Some(b"b".to_vec())
        );
    }

    #[test]
    fn upper_bound_never_reads_excluded_index_partitions() {
        use crate::iterator::merge::SeekableIterator;
        use crate::types::InternalKey;
        use std::fs;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upper_bound_partitions.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..40u32 {
            let mut uk = format!("k{:04}_", i * 3).into_bytes();
            uk.resize(2048, b'x');
            let ik = InternalKey::new(&uk, 10, ValueType::Value);
            builder.add(ik.as_bytes(), b"v").unwrap();
        }
        builder.finish().unwrap();

        let clean = TableReader::open(&path).unwrap();
        assert!(clean.partitions.len() >= 2);
        let part0_sep_uk = clean.partitions[0].0.prefix.to_vec();
        let part1_leaf = clean.partitions[1].1;

        // Corrupt partition 1's index leaf CRC on disk so any read of leaf 1 errors.
        let mut bytes = fs::read(&path).unwrap();
        let crc_pos = (part1_leaf.offset + part1_leaf.size + 1) as usize;
        bytes[crc_pos] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let reader = Arc::new(TableReader::open(&path).unwrap());
        let mut iter = TableIterator::new(reader);
        SeekableIterator::set_bounds(&mut iter, None, Some(&part0_sep_uk));

        // 1. Out-of-bound forward seek + next() must not read partition 1's leaf.
        let mut oob = b"k9999_".to_vec();
        oob.resize(2048, b'x');
        let seek_oob = InternalKey::new(&oob, 100, ValueType::Value);
        iter.seek(seek_oob.as_bytes());
        assert!(iter.iter_error().is_none());
        assert!(iter.next().is_none());
        assert!(iter.iter_error().is_none());

        // 2. Out-of-bound seek_for_prev and seek_to_last clamp to upper_bound -> partition 0.
        iter.seek_for_prev(seek_oob.as_bytes());
        assert!(iter.iter_error().is_none());
        assert!(iter.current().is_some());

        SeekableIterator::seek_to_last(&mut iter);
        assert!(iter.iter_error().is_none());
        assert!(iter.current().is_some());

        // 3. Full forward scan across partition 0 stops before loading partition 1's leaf.
        SeekableIterator::seek_to_first(&mut iter);
        while iter.next().is_some() {}
        assert!(
            iter.iter_error().is_none(),
            "forward scan at upper_bound must not read excluded index partition: {:?}",
            iter.iter_error()
        );
    }
}
