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
    BLOCK_TRAILER_SIZE, BlockHandle, CompressionType, FOOTER_SIZE, RANGE_DEL_BLOCK_NAME,
    decode_footer, decode_index_value,
};
use crate::stats::DbStats;
use crate::types::{InternalKeyRef, SequenceNumber, ValueType};
use ruc::*;

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

/// Bloom filter data and range-del handle read from SST metaindex.
struct MetaIndexData {
    bloom: Option<Vec<u8>>,
    prefix: Option<Vec<u8>>,
    range_del_handle: Option<BlockHandle>,
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
    stats: Option<Arc<DbStats>>,
    /// Cached index entries, shared across all TableIterators for this file.
    /// Populated once on first access, then reused (Arc for zero-copy sharing).
    index_entry_cache: OnceLock<IndexEntries>,
    /// Cached range tombstones for this SST file as a pre-fragmented index.
    /// Populated once on first max_covering_tombstone_seq call, then reused.
    /// O(log T) binary search instead of O(T) linear scan.
    range_tombstone_cache: OnceLock<Arc<crate::iterator::range_del::FragmentedRangeTombstoneList>>,
    /// Handle to the range-deletion block (if present in metaindex).
    range_del_handle: Option<BlockHandle>,
}

impl TableReader {
    /// Open an SST file for reading.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_all(path, 0, None, None)
    }

    /// Open an SST file for reading with a known file number (for cache keying).
    pub fn open_with_number(path: &Path, file_number: u64) -> Result<Self> {
        Self::open_with_all(path, file_number, None, None)
    }

    /// Open with file number and optional block cache.
    pub fn open_full(
        path: &Path,
        file_number: u64,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Result<Self> {
        Self::open_with_all(path, file_number, block_cache, None)
    }

    /// Open with file number, optional block cache, and optional stats.
    pub fn open_with_all(
        path: &Path,
        file_number: u64,
        block_cache: Option<Arc<BlockCache>>,
        stats: Option<Arc<DbStats>>,
    ) -> Result<Self> {
        let mut file = File::open(path).c(d!())?;
        let file_size = file.metadata().c(d!())?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(eg!(Error::Corruption(format!(
                "SST file too small: {} bytes",
                file_size
            ))));
        }

        // Read footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).c(d!())?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf).c(d!())?;
        let (metaindex_handle, index_handle) = decode_footer(&footer_buf).c(d!())?;

        // Read index block
        let index_data = Self::read_block_data(&mut file, &index_handle).c(d!())?;
        let index_block = Block::from_vec(index_data).c(d!())?;

        // Read filters and range-del handle from metaindex
        let meta = Self::read_metaindex(&mut file, &metaindex_handle).c(d!())?;

        Ok(Self {
            path: path.to_path_buf(),
            file_size,
            file_number,
            index_block,
            filter_data: meta.bloom,
            prefix_filter_data: meta.prefix,
            file: Mutex::new(file),
            block_cache,
            stats,
            index_entry_cache: OnceLock::new(),
            range_tombstone_cache: OnceLock::new(),
            range_del_handle: meta.range_del_handle,
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
        let block_handle = match self.find_data_block(key).c(d!())? {
            Some(h) => h,
            None => return Ok(None),
        };

        // Read and search the data block
        let block_data = self.read_block_cached(&block_handle).c(d!())?;
        let block = Block::new(block_data).c(d!())?;

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

        let block_data = self.read_block_cached(&handle).c(d!())?;
        let block = Block::new(block_data).c(d!())?;

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

    /// Like `get_internal` but also returns the sequence number of the found entry.
    /// Returns `Some((result, entry_seq))` if found, `None` if key not in this table.
    pub fn get_internal_with_seq(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<(Option<Vec<u8>>, SequenceNumber)>> {
        use crate::types::InternalKey;

        // Check bloom filter with user key
        if let Some(ref filter) = self.filter_data
            && !BloomFilter::key_may_match(user_key, filter)
        {
            return Ok(None);
        }

        let seek_key = InternalKey::new(user_key, sequence, ValueType::Value);

        use crate::types::compare_internal_key;

        let handle = match self
            .index_block
            .seek_by(seek_key.as_bytes(), compare_internal_key)
        {
            Some((_idx_key, handle_bytes)) => BlockHandle::decode(&handle_bytes),
            None => return Ok(None),
        };

        let block_data = self.read_block_cached(&handle).c(d!())?;
        let block = Block::new(block_data).c(d!())?;

        match block.seek_by(seek_key.as_bytes(), compare_internal_key) {
            Some((encoded_ikey, value)) if encoded_ikey.len() >= 8 => {
                let ik = InternalKeyRef::new(&encoded_ikey);
                if ik.user_key() == user_key {
                    let entry_seq = ik.sequence();
                    return Ok(Some((
                        match ik.value_type() {
                            ValueType::Value => Some(value),
                            ValueType::Deletion | ValueType::RangeDeletion => None,
                        },
                        entry_seq,
                    )));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Find the highest-seq range tombstone covering `user_key` with seq <= `read_seq`.
    /// Returns 0 if none found. Only meaningful for SSTs that contain range deletions.
    ///
    /// Range tombstones are cached per-SST on first access, so subsequent calls
    /// are O(T) in the number of tombstones rather than O(blocks * entries).
    pub fn max_covering_tombstone_seq(
        &self,
        user_key: &[u8],
        read_seq: SequenceNumber,
    ) -> Result<SequenceNumber> {
        let tombstones = self.cached_range_tombstones().c(d!())?;
        Ok(tombstones.max_covering_tombstone_seq(user_key, read_seq))
    }

    /// Return all range tombstones as (begin, end, seq) triples.
    /// Delegates to `cached_range_tombstones()` — no extra I/O after first call.
    #[allow(clippy::type_complexity)]
    pub fn get_range_tombstones(
        &self,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>, crate::types::SequenceNumber)>> {
        let cached = self.cached_range_tombstones().c(d!())?;
        Ok(cached.tombstones())
    }

    /// Get cached range tombstones as a pre-fragmented index (O(log T) lookup).
    /// Populated once on first access.
    fn cached_range_tombstones(
        &self,
    ) -> Result<Arc<crate::iterator::range_del::FragmentedRangeTombstoneList>> {
        if let Some(cached) = self.range_tombstone_cache.get() {
            return Ok(cached.clone());
        }

        let mut triples = Vec::new();

        if let Some(ref handle) = self.range_del_handle {
            // New path: read from dedicated range-del block
            let block_data = self.read_block_cached(handle).c(d!())?;
            let block = Block::new(block_data).c(d!())?;
            for (k, v) in block.iter() {
                if k.len() < 8 {
                    continue;
                }
                let ikr = InternalKeyRef::new(&k);
                if ikr.value_type() == ValueType::RangeDeletion {
                    triples.push((ikr.user_key().to_vec(), v, ikr.sequence()));
                }
            }
        } else {
            // Backward compatibility: old SST format without range-del block
            for (_, handle_bytes) in self.index_block.iter() {
                let handle = BlockHandle::decode(&handle_bytes);
                let block_data = self.read_block_cached(&handle).c(d!())?;
                let block = Block::new(block_data).c(d!())?;

                for (k, v) in block.iter() {
                    if k.len() < 8 {
                        continue;
                    }
                    let ikr = InternalKeyRef::new(&k);
                    if ikr.value_type() != ValueType::RangeDeletion {
                        continue;
                    }
                    triples.push((ikr.user_key().to_vec(), v, ikr.sequence()));
                }
            }
        }

        let cached = Arc::new(
            crate::iterator::range_del::FragmentedRangeTombstoneList::new(triples),
        );
        // Race is benign — worst case we build twice
        let _ = self.range_tombstone_cache.set(cached.clone());
        Ok(cached)
    }

    /// Iterate over all key-value pairs in the table.
    pub fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = Vec::new();

        for (_, handle_bytes) in self.index_block.iter() {
            let handle = BlockHandle::decode(&handle_bytes);
            let block_data = self.read_block_cached(&handle).c(d!())?;
            let block = Block::new(block_data).c(d!())?;
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
        file.seek(SeekFrom::Start(handle.offset)).c(d!())?;
        let mut data = vec![0u8; handle.size as usize];
        file.read_exact(&mut data).c(d!())?;

        // Read and verify trailer
        let mut trailer = [0u8; BLOCK_TRAILER_SIZE];
        file.read_exact(&mut trailer).c(d!())?;

        let compression_type = CompressionType::from_u8(trailer[0])
            .ok_or_else(|| Error::Corruption("unknown compression type".to_string()))
            .c(d!())?;

        let stored_crc = u32::from_le_bytes(trailer[1..5].try_into().unwrap());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data);
        hasher.update(&[trailer[0]]);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(eg!(Error::Corruption(format!(
                "block CRC mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            ))));
        }

        // Decompress if needed
        let data = match compression_type {
            CompressionType::Lz4 => lz4_flex::decompress_size_prepended(&data)
                .map_err(|e| Error::Corruption(format!("LZ4 decompression error: {}", e)))
                .c(d!())?,
            CompressionType::Zstd => zstd::stream::decode_all(data.as_slice())
                .map_err(|e| Error::Corruption(format!("Zstd decompression error: {}", e)))
                .c(d!())?,
            CompressionType::None => data,
        };

        Ok(data)
    }

    fn read_metaindex(file: &mut File, metaindex_handle: &BlockHandle) -> Result<MetaIndexData> {
        if metaindex_handle.size == 0 {
            return Ok(MetaIndexData {
                bloom: None,
                prefix: None,
                range_del_handle: None,
            });
        }

        let metaindex_data = Self::read_block_data(file, metaindex_handle).c(d!())?;
        let metaindex = Block::from_vec(metaindex_data).c(d!())?;

        let mut bloom = None;
        let mut prefix = None;
        let mut range_del_handle = None;

        for (key, value) in metaindex.iter() {
            if key == b"filter.bloom" {
                let handle = BlockHandle::decode(&value);
                bloom = Some(Self::read_block_data(file, &handle).c(d!())?);
            } else if key == b"filter.prefix" {
                let handle = BlockHandle::decode(&value);
                prefix = Some(Self::read_block_data(file, &handle).c(d!())?);
            } else if key == RANGE_DEL_BLOCK_NAME.as_bytes() {
                range_del_handle = Some(BlockHandle::decode(&value));
            }
        }

        Ok(MetaIndexData {
            bloom,
            prefix,
            range_del_handle,
        })
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
            if let Some(ref s) = self.stats {
                s.record_cache_hit();
            }
            return Ok(cached);
        }

        if let Some(ref s) = self.stats
            && self.block_cache.is_some()
        {
            s.record_cache_miss();
        }

        let mut file = self.open_file().c(d!())?;
        let data = Self::read_block_data(&mut file, handle).c(d!())?;

        if let Some(ref cache) = self.block_cache {
            return Ok(cache.insert(self.file_number, handle.offset, data));
        }

        Ok(Arc::new(data))
    }

    /// Pin this file's first data block in cache as non-evictable (for L0 files).
    /// Pre-populates the index entry cache and pins the first data block
    /// so that init_heap's first peek() always hits cache.
    pub fn pin_metadata_in_cache(&self) {
        // Populate the OnceLock index entry cache eagerly
        let entries = self.cached_index_entries();
        // Pin first data block in cache (non-evictable)
        if let Some(entry) = entries.first()
            && let Some(ref cache) = self.block_cache
            && cache.get(self.file_number, entry.handle.offset).is_none()
            && let Ok(mut file) = self.open_file()
            && let Ok(data) = Self::read_block_data(&mut file, &entry.handle)
        {
            cache.insert_pinned(self.file_number, entry.handle.offset, data);
        }
    }

    /// Hint the OS to prefetch the given file range into page cache.
    /// Uses `posix_fadvise` on Linux; no-op on other platforms.
    fn advise_willneed(&self, offset: u64, len: u64) {
        #[cfg(target_os = "linux")]
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
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (offset, len);
        }
    }

    /// Get a file handle for reading. Uses the held file via mutex.
    fn open_file(&self) -> Result<MutexGuard<'_, File>> {
        self.file
            .lock()
            .map_err(|_| eg!(Error::Corruption("file mutex poisoned".to_string())))
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
        // Check upper bound before returning entry
        if let Some(ref ub) = self.upper_bound {
            if crate::types::user_key(&self.block_cursor_key) >= ub.as_slice() {
                return None;
            }
        }
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

            match crate::sst::block::decode_entry_reuse(
                block_data,
                offset,
                &mut self.block_cursor_key,
            ) {
                Some((_, _, next_off)) => {
                    if compare(&self.block_cursor_key, target) != std::cmp::Ordering::Less {
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
                // Use seek_for_prev_by to find the target entry efficiently
                match block.seek_for_prev_by(target, compare_internal_key) {
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
                                compare_internal_key(k, target) != std::cmp::Ordering::Greater
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
                }
            }

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
    fn find_restart_for_key<F: Fn(&[u8], &[u8]) -> std::cmp::Ordering>(
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
                if compare(&first_key, key) != std::cmp::Ordering::Greater {
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

        let prev_block_index = current_block_index - 1;
        self.ensure_index();
        let handle = self.index_entries.as_ref().unwrap()[prev_block_index].handle;

        if let Ok(data) = self.reader.read_block_cached(&handle)
            && let Ok(block) = Block::new(data)
        {
            // Load only the last restart segment of the previous block
            let last_restart = block.num_restarts().saturating_sub(1);
            self.current_block_entries = block.iter_restart_segment(last_restart);
            if self.current_block_entries.is_empty() {
                return None;
            }
            self.block_pos = self.current_block_entries.len() - 1;
            self.index_pos = prev_block_index + 1;
            self.current_restart_index = last_restart;
            self.backward_block = Some(block);
            self.backward_block_index = prev_block_index;
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

            // Skip blocks whose first_key user key >= upper_bound
            if let Some(ref ub) = self.upper_bound {
                if let Some(ref fk) = index_entries[block_idx].first_key {
                    if crate::types::user_key(fk) >= ub.as_slice() {
                        self.index_pos = index_entries.len();
                        return false;
                    }
                }
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
                        self.err =
                            Some(format!("block decode error at index {block_idx}: {e}"));
                        return false;
                    }
                },
                Err(e) => {
                    self.err =
                        Some(format!("block read error at index {block_idx}: {e}"));
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

    fn current(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        TableIterator::current(self)
    }

    fn prev(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        TableIterator::prev(self)
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        TableIterator::seek_for_prev(self, target);
    }

    fn seek_to_first(&mut self) {
        // Reset to the beginning of the table
        self.ensure_index();
        self.index_pos = 0;
        self.current_block_entries.clear();
        self.block_pos = 0;
        self.current_block = None;
        self.at_first_key_from_index = false;
    }

    fn seek_to_last(&mut self) {
        self.ensure_index();
        let index_entries = self.index_entries.as_ref().unwrap();
        if index_entries.is_empty() {
            return;
        }
        // Load only the last restart segment of the last block
        let last_idx = index_entries.len() - 1;
        let handle = index_entries[last_idx].handle;
        if let Ok(data) = self.reader.read_block_cached(&handle)
            && let Ok(block) = Block::new(data)
        {
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
        self.current_block = None;
        self.at_first_key_from_index = false;
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
                    // Check upper bound before returning entry
                    if let Some(ref ub) = self.upper_bound {
                        if crate::types::user_key(&self.block_cursor_key) >= ub.as_slice() {
                            return false;
                        }
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
                if let Some(ref ub) = self.upper_bound {
                    if crate::types::user_key(k) >= ub.as_slice() {
                        return false;
                    }
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
