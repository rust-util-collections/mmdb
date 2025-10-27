//! SST table reader: reads key-value pairs from an SST file.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::cache::block_cache::BlockCache;
use crate::error::{Error, Result};
use crate::sst::block::Block;
use crate::sst::filter::BloomFilter;
use crate::sst::format::{
    BLOCK_TRAILER_SIZE, BlockHandle, CompressionType, FOOTER_SIZE, decode_footer,
    decode_index_value,
};
use crate::types::{InternalKeyRef, SequenceNumber, ValueType};

/// Parsed index entry: separator key + block handle + optional first key.
pub struct IndexEntry {
    /// Separator key (last key of the data block).
    pub separator_key: Vec<u8>,
    /// Handle pointing to the data block.
    pub handle: BlockHandle,
    /// First key of the data block (for deferred block reads). None for old SST format.
    pub first_key: Option<Vec<u8>>,
}

/// Shared index entries parsed from the SST index block.
type IndexEntries = Arc<Vec<IndexEntry>>;

/// Bloom filter data read from SST metaindex.
struct FilterData {
    bloom: Option<Vec<u8>>,
    prefix: Option<Vec<u8>>,
}

/// Reader for an SST file.
pub struct TableReader {
    path: PathBuf,
    file_size: u64,
    file_number: u64,
    index_block: Block,
    filter_data: Option<Vec<u8>>,
    prefix_filter_data: Option<Vec<u8>>,
    file: Mutex<File>,
    block_cache: Option<Arc<BlockCache>>,
    /// Cached index entries, shared across all TableIterators for this file.
    /// Populated once on first access, then reused (Arc for zero-copy sharing).
    index_entry_cache: OnceLock<IndexEntries>,
}

impl TableReader {
    /// Open an SST file for reading.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_full(path, 0, None)
    }

    /// Open an SST file for reading with a known file number (for cache keying).
    pub fn open_with_number(path: &Path, file_number: u64) -> Result<Self> {
        Self::open_full(path, file_number, None)
    }

    /// Open with file number and optional block cache.
    pub fn open_full(
        path: &Path,
        file_number: u64,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::Corruption(format!(
                "SST file too small: {} bytes",
                file_size
            )));
        }

        // Read footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;
        let (metaindex_handle, index_handle) = decode_footer(&footer_buf)?;

        // Read index block
        let index_data = Self::read_block_data(&mut file, &index_handle)?;
        let index_block = Block::from_vec(index_data)?;

        // Read filters from metaindex
        let filters = Self::read_filters(&mut file, &metaindex_handle)?;

        Ok(Self {
            path: path.to_path_buf(),
            file_size,
            file_number,
            index_block,
            filter_data: filters.bloom,
            prefix_filter_data: filters.prefix,
            file: Mutex::new(file),
            block_cache,
            index_entry_cache: OnceLock::new(),
        })
    }

    /// Get cached index entries (shared across all TableIterators for this file).
    /// Populated once on first access, then reused via Arc.
    /// Parses extended index values (BlockHandle + optional first_key).
    pub fn cached_index_entries(&self) -> IndexEntries {
        self.index_entry_cache
            .get_or_init(|| {
                Arc::new(
                    self.index_block
                        .iter()
                        .map(|(k, v)| {
                            let (handle, first_key) = decode_index_value(&v);
                            IndexEntry {
                                separator_key: k,
                                handle,
                                first_key: first_key.map(|fk| fk.to_vec()),
                            }
                        })
                        .collect(),
                )
            })
            .clone()
    }

    /// Look up a key in the SST (exact byte match). Returns the value if found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check bloom filter first
        if let Some(ref filter) = self.filter_data
            && !BloomFilter::key_may_match(key, filter)
        {
            return Ok(None);
        }

        // Search index block for the data block that might contain the key
        let block_handle = match self.find_data_block(key)? {
            Some(h) => h,
            None => return Ok(None),
        };

        // Read and search the data block
        let block_data = self.read_block_cached(&block_handle)?;
        let block = Block::new(block_data)?;

        match block.seek(key) {
            Some((found_key, value)) if found_key.as_slice() == key => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    /// Look up a user key in an SST that stores internal keys.
    /// Finds the latest entry for `user_key` with sequence <= `sequence`.
    /// Returns:
    /// - `Some(Some(value))` if a Value entry is found
    /// - `Some(None)` if a Deletion tombstone is found
    /// - `None` if the key doesn't exist in this table
    pub fn get_internal(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<Option<Vec<u8>>>> {
        use crate::types::InternalKey;

        // Check bloom filter with user key
        if let Some(ref filter) = self.filter_data
            && !BloomFilter::key_may_match(user_key, filter)
        {
            return Ok(None);
        }

        // Construct a seek key: (user_key, sequence, Value).
        // With inverted-BE encoding, lex order = logical order, so seeking to this
        // key in the index block finds the right data block via binary search.
        let seek_key = InternalKey::new(user_key, sequence, ValueType::Value);

        use crate::types::compare_internal_key;

        // Use index block seek to find the data block that may contain our key.
        let handle = match self
            .index_block
            .seek_by(seek_key.as_bytes(), compare_internal_key)
        {
            Some((_idx_key, handle_bytes)) => BlockHandle::decode(&handle_bytes),
            None => return Ok(None),
        };

        let block_data = self.read_block_cached(&handle)?;
        let block = Block::new(block_data)?;

        // Seek within the data block. The first entry >= seek_key with matching user_key
        // is our answer (because entries are sorted user_key ASC, seq DESC).
        match block.seek_by(seek_key.as_bytes(), compare_internal_key) {
            Some((encoded_ikey, value)) if encoded_ikey.len() >= 8 => {
                let ik = InternalKeyRef::new(&encoded_ikey);
                if ik.user_key() == user_key {
                    return Ok(Some(match ik.value_type() {
                        ValueType::Value => Some(value),
                        ValueType::Deletion | ValueType::RangeDeletion => None,
                    }));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Iterate over all key-value pairs in the table.
    pub fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = Vec::new();

        for (_, handle_bytes) in self.index_block.iter() {
            let handle = BlockHandle::decode(&handle_bytes);
            let block_data = self.read_block_cached(&handle)?;
            let block = Block::new(block_data)?;
            for entry in block.iter() {
                result.push(entry);
            }
        }

        Ok(result)
    }

    /// Find the data block handle that might contain the given key.
    fn find_data_block(&self, key: &[u8]) -> Result<Option<BlockHandle>> {
        match self.index_block.seek(key) {
            Some((_idx_key, handle_bytes)) => {
                let handle = BlockHandle::decode(&handle_bytes);
                Ok(Some(handle))
            }
            None => Ok(None),
        }
    }

    fn read_block_data(file: &mut File, handle: &BlockHandle) -> Result<Vec<u8>> {
        file.seek(SeekFrom::Start(handle.offset))?;
        let mut data = vec![0u8; handle.size as usize];
        file.read_exact(&mut data)?;

        // Read and verify trailer
        let mut trailer = [0u8; BLOCK_TRAILER_SIZE];
        file.read_exact(&mut trailer)?;

        let compression_type = CompressionType::from_u8(trailer[0])
            .ok_or_else(|| Error::Corruption("unknown compression type".to_string()))?;

        let stored_crc = u32::from_le_bytes(trailer[1..5].try_into().unwrap());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data);
        hasher.update(&[trailer[0]]);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(Error::Corruption(format!(
                "block CRC mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            )));
        }

        // Decompress if needed
        let data = match compression_type {
            CompressionType::Lz4 => lz4_flex::decompress_size_prepended(&data)
                .map_err(|e| Error::Corruption(format!("LZ4 decompression error: {}", e)))?,
            CompressionType::Zstd => zstd::stream::decode_all(data.as_slice())
                .map_err(|e| Error::Corruption(format!("Zstd decompression error: {}", e)))?,
            CompressionType::None => data,
        };

        Ok(data)
    }

    fn read_filters(file: &mut File, metaindex_handle: &BlockHandle) -> Result<FilterData> {
        if metaindex_handle.size == 0 {
            return Ok(FilterData {
                bloom: None,
                prefix: None,
            });
        }

        let metaindex_data = Self::read_block_data(file, metaindex_handle)?;
        let metaindex = Block::from_vec(metaindex_data)?;

        let mut bloom = None;
        let mut prefix = None;

        for (key, value) in metaindex.iter() {
            if key == b"filter.bloom" {
                let handle = BlockHandle::decode(&value);
                bloom = Some(Self::read_block_data(file, &handle)?);
            } else if key == b"filter.prefix" {
                let handle = BlockHandle::decode(&value);
                prefix = Some(Self::read_block_data(file, &handle)?);
            }
        }

        Ok(FilterData { bloom, prefix })
    }

    /// Check if a prefix may exist in this SST file using the prefix bloom filter.
    /// Returns `true` if the prefix might be present (or if no prefix bloom exists).
    pub fn prefix_may_match(&self, prefix: &[u8]) -> bool {
        match self.prefix_filter_data {
            Some(ref filter) => BloomFilter::key_may_match(prefix, filter),
            None => true, // No prefix bloom — conservatively assume present
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Read a block, consulting the cache first if available.
    /// Returns Arc<Vec<u8>> — zero-copy on cache hit.
    fn read_block_cached(&self, handle: &BlockHandle) -> Result<Arc<Vec<u8>>> {
        if let Some(ref cache) = self.block_cache
            && let Some(cached) = cache.get(self.file_number, handle.offset)
        {
            return Ok(cached);
        }

        let mut file = self.open_file()?;
        let data = Self::read_block_data(&mut file, handle)?;

        if let Some(ref cache) = self.block_cache {
            return Ok(cache.insert(self.file_number, handle.offset, data));
        }

        Ok(Arc::new(data))
    }

    /// Pin this file's index block data and first data block in cache (for L0 files).
    /// Pre-populates the index entry cache and pre-reads the first data block
    /// so that init_heap's first peek() hits the block cache.
    pub fn pin_metadata_in_cache(&self) {
        // Populate the OnceLock index entry cache eagerly
        let entries = self.cached_index_entries();
        // Also pre-read first data block into cache
        if let Some(entry) = entries.first() {
            let _ = self.read_block_cached(&entry.handle);
        }
    }

    /// Hint the OS to prefetch the given file range into page cache.
    fn advise_willneed(&self, offset: u64, len: u64) {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            if let Ok(file) = self.open_file() {
                unsafe {
                    libc::posix_fadvise(
                        file.as_raw_fd(),
                        offset as i64,
                        len as i64,
                        libc::POSIX_FADV_WILLNEED,
                    );
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (offset, len);
        }
    }

    /// Get a file handle for reading. Uses the held file via mutex.
    fn open_file(&self) -> Result<MutexGuard<'_, File>> {
        self.file
            .lock()
            .map_err(|_| Error::Corruption("file mutex poisoned".to_string()))
    }
}

/// Streaming iterator over an SST file — reads blocks on demand.
/// Memory usage: O(1 block) instead of O(entire table).
///
/// Forward iteration uses a zero-alloc cursor that decodes entries one at a time
/// directly from the raw block data. Backward iteration (`seek_for_prev`, `prev`)
/// materializes the block into a Vec for random access.
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

    // --- Backward iteration (materialized) ---
    /// Materialized block entries — only populated by seek_for_prev/prev.
    current_block_entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Position within materialized block.
    block_pos: usize,
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
        }
    }

    /// Ensure index entries are loaded. Returns a reference to them.
    fn ensure_index(&mut self) -> &IndexEntries {
        if self.index_entries.is_none() {
            self.index_entries = Some(self.reader.cached_index_entries());
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

    /// Materialize the deferred block: load the data block for deferred_index_pos.
    fn materialize_deferred_block(&mut self) {
        if let Some(ref index_entries) = self.index_entries
            && let Ok(data) = self
                .reader
                .read_block_cached(&index_entries[self.deferred_index_pos].handle)
            && let Ok(block) = Block::new(data)
        {
            self.set_block_for_cursor(block);
        }
    }

    /// Decode the next entry from the current block using the cursor.
    /// Returns (key_clone, value_clone) — key is cloned from the reusable buffer,
    /// value is copied from the block data slice.
    fn cursor_next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let block = self.current_block.as_ref()?;
        if self.block_cursor_offset >= self.block_data_end {
            return None;
        }
        let data = block.data();
        let (value_start, value_len, next_offset) = crate::sst::block::decode_entry_reuse(
            data,
            self.block_cursor_offset,
            &mut self.block_cursor_key,
        )?;
        self.block_cursor_offset = next_offset;
        Some((
            self.block_cursor_key.clone(),
            data[value_start..value_start + value_len].to_vec(),
        ))
    }

    /// Seek to the first entry >= target using the index block for O(log N) lookup.
    pub fn seek(&mut self, target: &[u8]) {
        use crate::types::compare_internal_key;

        self.ensure_index();
        let index_entries = self.index_entries.as_ref().unwrap();

        // Quick check: if target > file's largest key, mark exhausted.
        if let Some(last) = index_entries.last()
            && compare_internal_key(target, &last.separator_key) == std::cmp::Ordering::Greater
        {
            self.index_pos = index_entries.len();
            self.current_block_entries.clear();
            self.block_pos = 0;
            return;
        }

        // Binary search index entries to find the first block that may contain target
        let idx = index_entries.partition_point(|entry| {
            compare_internal_key(&entry.separator_key, target) == std::cmp::Ordering::Less
        });

        self.index_pos = idx;
        self.current_block = None;
        self.current_block_entries.clear();
        self.block_pos = 0;

        // Load the found block and seek within it using the cursor
        if self.index_pos < index_entries.len() {
            let entry = &index_entries[self.index_pos];
            self.index_pos += 1;

            // Deferred block read: if target <= first_key, position without I/O
            if let Some(ref first_key) = entry.first_key
                && compare_internal_key(target, first_key) != std::cmp::Ordering::Greater
            {
                self.at_first_key_from_index = true;
                self.deferred_index_pos = self.index_pos - 1;
                self.block_cursor_key.clear();
                self.block_cursor_key.extend_from_slice(first_key);
                self.current_block = None;
                return;
            }

            if let Ok(data) = self.reader.read_block_cached(&entry.handle)
                && let Ok(block) = Block::new(data)
            {
                self.seek_within_block(block, target, compare_internal_key);
            }
        }
    }

    /// Position the cursor at the first entry >= `target` within `block`.
    fn seek_within_block<F: Fn(&[u8], &[u8]) -> std::cmp::Ordering>(
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
            match crate::sst::block::decode_entry_reuse(block_data, rp, &mut tmp_key) {
                Some(_) => {
                    if compare(&tmp_key, target) == std::cmp::Ordering::Less {
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

        // Scan entries, tracking the offset of each entry
        self.block_cursor_key.clear();
        let mut offset = start;
        while offset < data_end {
            // Save state before decoding — if this entry >= target, we want cursor here
            let entry_offset = offset;
            match crate::sst::block::decode_entry_reuse(
                block_data,
                offset,
                &mut self.block_cursor_key,
            ) {
                Some((_, _, next_off)) => {
                    if compare(&self.block_cursor_key, target) != std::cmp::Ordering::Less {
                        // Found first entry >= target.
                        // block_cursor_key already contains this entry's key.
                        // Set cursor_offset to NEXT entry so cursor_next() advances past it.
                        // But we need to return THIS entry on the next cursor_next() call.
                        // Solution: store the current decoded state and use a "pending" entry.
                        // Simpler: set offset to entry_offset and clear key to prev state.
                        // Actually simplest: we already decoded this entry into key_buf.
                        // We'll store the entry offset and re-scan from start to rebuild
                        // key_buf to the state *before* this entry, then cursor_next()
                        // will re-decode it.

                        // Re-scan from start to entry_offset to rebuild prefix state
                        self.block_cursor_key.clear();
                        let mut scan = start;
                        while scan < entry_offset {
                            if let Some((_, _, n)) = crate::sst::block::decode_entry_reuse(
                                block_data,
                                scan,
                                &mut self.block_cursor_key,
                            ) {
                                scan = n;
                            } else {
                                break;
                            }
                        }
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
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        use crate::types::compare_internal_key;

        self.ensure_index();
        let index_entries = self.index_entries.as_ref().unwrap();

        let idx = index_entries.partition_point(|entry| {
            compare_internal_key(&entry.separator_key, target) == std::cmp::Ordering::Less
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

            let handle = index_entries[try_idx].handle;

            if let Ok(data) = self.reader.read_block_cached(&handle)
                && let Ok(block) = Block::new(data)
            {
                let entries: Vec<(Vec<u8>, Vec<u8>)> = block.iter().collect();
                // Find the last entry <= target in this block
                let pos = entries.partition_point(|(k, _)| {
                    compare_internal_key(k, target) != std::cmp::Ordering::Greater
                });

                if pos > 0 {
                    // Found an entry <= target in this block
                    self.index_pos = try_idx + 1; // next block to load on forward iteration
                    self.current_block_entries = entries;
                    self.block_pos = pos - 1;
                    found = true;
                    break;
                }
            }

            // No entry <= target in this block; try the previous block
            if try_idx == 0 {
                break;
            }
            try_idx -= 1;
        }

        if !found {
            // No entry <= target in the entire table
            self.index_pos = index_entries.len();
            self.current_block_entries.clear();
            self.block_pos = 0;
        }
    }

    /// Move to the previous entry. Returns the entry at the new position,
    /// or None if we've moved before the first entry.
    pub fn prev(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        // If we have a previous entry in the current block, just decrement
        if self.block_pos > 0 {
            self.block_pos -= 1;
            return Some(self.current_block_entries[self.block_pos].clone());
        }

        // Need to load the previous block.
        // index_pos points to the *next* block to load for forward iteration.
        // The current block is at index_pos - 1. The previous block is index_pos - 2.
        // But we need to be careful: after seek_for_prev, index_pos = try_idx + 1,
        // so the current block index is index_pos - 1.
        // We want the block *before* the current one.
        let current_block_index = if self.index_pos > 0 {
            self.index_pos - 1
        } else {
            return None; // Already at or before the first block
        };

        if current_block_index == 0 {
            return None; // Current block is the first block, no previous
        }

        let prev_block_index = current_block_index - 1;
        self.ensure_index();
        let handle = self.index_entries.as_ref().unwrap()[prev_block_index].handle;

        if let Ok(data) = self.reader.read_block_cached(&handle)
            && let Ok(block) = Block::new(data)
        {
            self.current_block_entries = block.iter().collect();
            if self.current_block_entries.is_empty() {
                return None;
            }
            self.block_pos = self.current_block_entries.len() - 1;
            self.index_pos = prev_block_index + 1;
            return Some(self.current_block_entries[self.block_pos].clone());
        }

        None
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
                    Err(_) => continue,
                },
                Err(_) => continue,
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

    fn next_into(&mut self, key_buf: &mut Vec<u8>, value_buf: &mut Vec<u8>) -> bool {
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
                if let Some((vs, vl, next)) = crate::sst::block::decode_entry_reuse(
                    data,
                    self.block_cursor_offset,
                    &mut self.block_cursor_key,
                ) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::table_builder::{TableBuildOptions, TableBuilder};

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
        while let Some((k, v)) = iter.next() {
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
        entries.sort_by(|(a, _), (b, _)| crate::types::compare_internal_key(a, b));

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
        let extract_uk = |ikey: &[u8]| -> Vec<u8> {
            crate::types::InternalKeyRef::new(ikey).user_key().to_vec()
        };

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
        let extract_uk = |ikey: &[u8]| -> Vec<u8> {
            crate::types::InternalKeyRef::new(ikey).user_key().to_vec()
        };

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
}
