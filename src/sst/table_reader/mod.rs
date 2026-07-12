//! SST table reader: reads key-value pairs from an SST file.
mod iterator;
pub use iterator::TableIterator;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::{Arc, OnceLock},
};

use parking_lot::{Mutex, MutexGuard};

use crate::cache::block_cache::BlockCache;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::range_del::FragmentedRangeTombstoneList;
use crate::sst::block::Block;
use crate::sst::filter::BloomFilter;
use crate::sst::format::{
    BLOCK_TRAILER_SIZE, BlockHandle, CompressionType, FOOTER_SIZE, PREFIX_FILTER_LEN_NAME,
    RANGE_DEL_BLOCK_NAME, decode_footer, decode_index_value_with_props,
};
use crate::stats::DbStats;
use crate::types::{InternalKeyRef, SequenceNumber, ValueType, compare_internal_key};

/// A range tombstone: (begin_key, end_key, sequence_number).
type RangeTombstoneEntry = (Vec<u8>, Vec<u8>, SequenceNumber);

/// Maximum allowed decompressed block size. Used by readers to reject
/// allocation bombs and by compaction to reserve enough output file numbers
/// when compacting compressed inputs.
pub(crate) const MAX_DECOMPRESSED_BLOCK_SIZE: usize = 64 * 1024 * 1024; // 64 MB

/// Parsed index entry: separator key + block handle + optional first key + block properties.
pub struct IndexEntry {
    /// Separator key (last key of the data block).
    pub separator_key: Vec<u8>,
    /// Handle pointing to the data block.
    pub handle: BlockHandle,
    /// First key of the data block (for deferred block reads). None for old SST format.
    pub first_key: Option<Vec<u8>>,
    /// Block properties collected during SST build. Empty for old SST format.
    pub properties: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Shared index entries parsed from the SST index block.
type IndexEntries = Arc<Vec<IndexEntry>>;

/// Bloom filter data and range-del handle read from SST metaindex.
struct MetaIndexData {
    bloom: Option<Vec<u8>>,
    prefix: Option<Vec<u8>>,
    prefix_len: Option<usize>,
    range_del_handle: Option<BlockHandle>,
}

/// Reader for an SST file.
pub struct TableReader {
    file_number: u64,
    index_block: Block,
    filter_data: Option<Vec<u8>>,
    prefix_filter_data: Option<Vec<u8>>,
    prefix_filter_len: Option<usize>,
    file: Mutex<File>,
    block_cache: Option<Arc<BlockCache>>,
    stats: Option<Arc<DbStats>>,
    /// Cached index entries, shared across all TableIterators for this file.
    /// Populated once on first access, then reused (Arc for zero-copy sharing).
    index_entry_cache: OnceLock<IndexEntries>,
    /// Cached range tombstones for this SST file as a pre-fragmented index.
    /// Populated once on first max_covering_tombstone_seq call, then reused.
    /// O(log T) binary search instead of O(T) linear scan.
    range_tombstone_cache: OnceLock<Arc<FragmentedRangeTombstoneList>>,
    /// Handle to the range-deletion block (if present in metaindex).
    range_del_handle: Option<BlockHandle>,
}

impl TableReader {
    fn validate_prefix_filter_metadata(
        prefix_filter: Option<&[u8]>,
        prefix_len: Option<usize>,
    ) -> Result<()> {
        if prefix_filter.is_some() && prefix_len == Some(0) {
            return Err(Error::corruption(
                "prefix filter metadata has zero prefix length".to_string(),
            ));
        }
        Ok(())
    }

    /// Open an SST file for reading.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_all(path, 0, None, None)
    }

    /// Open with file number, optional block cache, and optional stats.
    pub fn open_with_all(
        path: &Path,
        file_number: u64,
        block_cache: Option<Arc<BlockCache>>,
        stats: Option<Arc<DbStats>>,
    ) -> Result<Self> {
        let mut file = File::open(path).ctx()?;
        let file_size = file.metadata().ctx()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::corruption(format!(
                "SST file too small: {} bytes",
                file_size
            )));
        }

        // Read footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).ctx()?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf).ctx()?;
        let (metaindex_handle, index_handle) = decode_footer(&footer_buf).ctx()?;

        // Read index block
        let index_data =
            Self::read_block_data_with_size(&mut file, &index_handle, file_size).ctx()?;
        let index_block = Block::from_vec(index_data).ctx()?;

        // Read filters and range-del handle from metaindex
        let meta = Self::read_metaindex(&mut file, &metaindex_handle, file_size).ctx()?;

        let reader = Self {
            file_number,
            index_block,
            filter_data: meta.bloom,
            prefix_filter_data: meta.prefix,
            prefix_filter_len: meta.prefix_len,
            file: Mutex::new(file),
            block_cache,
            stats,
            index_entry_cache: OnceLock::new(),
            range_tombstone_cache: OnceLock::new(),
            range_del_handle: meta.range_del_handle,
        };

        // Eagerly warm the range-tombstone cache at open time for files using
        // the dedicated range-del block (any file written by current code
        // that actually has range deletions — `write_range_del_block` only
        // produces a non-default handle in that case). This removes the
        // lazy-first-access gap that otherwise lets `file_overlaps_extent()`
        // perform blocking disk I/O from inside compaction's pick/install
        // phases, which run under the DB's write-serializing lock (see
        // `is_bottommost_level` and `outputs_overlap_unexpected_current_files`).
        // The legacy fallback path (no dedicated block; `range_del_handle` is
        // `None`) is intentionally left lazy — populating it requires
        // scanning every data block in the file, which would regress open()
        // for every old-format file regardless of whether it actually has
        // range deletions.
        if reader.range_del_handle.is_some() {
            reader.cached_range_tombstones().ctx()?;
        }

        Ok(reader)
    }

    /// Get cached index entries (shared across all TableIterators for this file).
    /// Populated once on first access, then reused via Arc.
    /// Parses extended index values (BlockHandle + optional first_key).
    pub fn cached_index_entries(&self) -> Result<IndexEntries> {
        if let Some(cached) = self.index_entry_cache.get() {
            return Ok(cached.clone());
        }
        let entries = Arc::new(Self::parse_index_entries(&self.index_block)?);
        // Benign race: worst case we parse twice.
        let _ = self.index_entry_cache.set(entries.clone());
        Ok(entries)
    }

    /// Parse index entries from the index block, propagating decode errors.
    fn parse_index_entries(index_block: &Block) -> Result<Vec<IndexEntry>> {
        let mut iter = index_block.iter();
        let mut entries = Vec::new();
        for (k, v) in &mut iter {
            let d = decode_index_value_with_props(&v).ctx()?;
            entries.push(IndexEntry {
                separator_key: k,
                handle: d.handle,
                first_key: d.first_key.map(|fk| fk.to_vec()),
                properties: d
                    .properties
                    .into_iter()
                    .map(|(n, p)| (n.to_vec(), p.to_vec()))
                    .collect(),
            });
        }
        // A corrupt index entry makes the iterator stop early with the error
        // recorded internally; without this check the truncated entry list
        // would silently hide every data block past the corruption point.
        if let Some(e) = iter.error() {
            return Err(e.clone()).ctx();
        }
        Ok(entries)
    }

    /// Look up a key in the SST (exact byte match). Returns the value if found.
    #[cfg(test)]
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check bloom filter first
        if let Some(ref filter) = self.filter_data
            && !BloomFilter::key_may_match(key, filter)
        {
            return Ok(None);
        }

        // Search index block for the data block that might contain the key
        let block_handle = match self.find_data_block(key).ctx()? {
            Some(h) => h,
            None => return Ok(None),
        };

        // Read and search the data block
        let block_data = self.read_block_cached(&block_handle).ctx()?;
        let block = Block::new(block_data).ctx()?;

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
    #[cfg(test)]
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

        // Use index block seek to find the data block that may contain our key.
        let handle = match self
            .index_block
            .seek_by(seek_key.as_bytes(), compare_internal_key)
            .ctx()?
        {
            Some((_idx_key, handle_bytes)) => BlockHandle::decode(&handle_bytes).ctx()?,
            None => return Ok(None),
        };

        let block_data = self.read_block_cached(&handle).ctx()?;
        let block = Block::new(block_data).ctx()?;

        // Seek within the data block. The first entry >= seek_key with matching user_key
        // is our answer (because entries are sorted user_key ASC, seq DESC).
        match block
            .seek_by(seek_key.as_bytes(), compare_internal_key)
            .ctx()?
        {
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
    /// When `fill_cache` is false, a cache miss does not populate the block cache.
    pub fn get_internal_with_seq(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
        fill_cache: bool,
    ) -> Result<Option<(Option<Vec<u8>>, SequenceNumber)>> {
        use crate::types::InternalKey;

        // Check bloom filter with user key
        if let Some(ref filter) = self.filter_data
            && !BloomFilter::key_may_match(user_key, filter)
        {
            return Ok(None);
        }

        let seek_key = InternalKey::new(user_key, sequence, ValueType::Value);

        let handle = match self
            .index_block
            .seek_by(seek_key.as_bytes(), compare_internal_key)
            .ctx()?
        {
            Some((_idx_key, handle_bytes)) => BlockHandle::decode(&handle_bytes).ctx()?,
            None => return Ok(None),
        };

        let block_data = self.read_block_cached_opt(&handle, fill_cache).ctx()?;
        let block = Block::new(block_data).ctx()?;

        match block
            .seek_by(seek_key.as_bytes(), compare_internal_key)
            .ctx()?
        {
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
        let tombstones = self.cached_range_tombstones().ctx()?;
        Ok(tombstones.max_covering_tombstone_seq(user_key, read_seq))
    }

    /// Return all range tombstones as (begin, end, seq) triples.
    /// Delegates to `cached_range_tombstones()` — no extra I/O after first call.
    pub fn get_range_tombstones(&self) -> Result<Vec<RangeTombstoneEntry>> {
        let cached = self.cached_range_tombstones().ctx()?;
        Ok(cached.tombstones())
    }

    /// Get cached range tombstones as a pre-fragmented index (O(log T) lookup).
    /// Populated once on first access.
    fn cached_range_tombstones(&self) -> Result<Arc<FragmentedRangeTombstoneList>> {
        if let Some(cached) = self.range_tombstone_cache.get() {
            return Ok(cached.clone());
        }

        let mut triples = Vec::new();

        if let Some(ref handle) = self.range_del_handle {
            // New path: read from dedicated range-del block
            let block_data = self.read_block_cached(handle).ctx()?;
            let block = Block::new(block_data).ctx()?;
            let mut iter = block.iter();
            for (k, v) in &mut iter {
                if k.len() < 8 {
                    continue;
                }
                let ikr = InternalKeyRef::new(&k);
                if ikr.value_type() == ValueType::RangeDeletion {
                    triples.push((ikr.user_key().to_vec(), v, ikr.sequence()));
                }
            }
            if let Some(e) = iter.error() {
                return Err(e.clone()).ctx();
            }
        } else {
            // Backward compatibility: old SST format without range-del block
            let mut index_iter = self.index_block.iter();
            for (_, handle_bytes) in &mut index_iter {
                let handle = BlockHandle::decode(&handle_bytes).ctx()?;
                let block_data = self.read_block_cached(&handle).ctx()?;
                let block = Block::new(block_data).ctx()?;

                let mut iter = block.iter();
                for (k, v) in &mut iter {
                    if k.len() < 8 {
                        continue;
                    }
                    let ikr = InternalKeyRef::new(&k);
                    if ikr.value_type() != ValueType::RangeDeletion {
                        continue;
                    }
                    triples.push((ikr.user_key().to_vec(), v, ikr.sequence()));
                }
                if let Some(e) = iter.error() {
                    return Err(e.clone()).ctx();
                }
            }
            if let Some(e) = index_iter.error() {
                return Err(e.clone()).ctx();
            }
        }

        let cached = Arc::new(FragmentedRangeTombstoneList::new(triples));
        // Race is benign — worst case we build twice
        let _ = self.range_tombstone_cache.set(cached.clone());
        Ok(cached)
    }

    /// Iterate over all key-value pairs in the table.
    #[cfg(test)]
    pub fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = Vec::new();

        let mut index_iter = self.index_block.iter();
        for (_, handle_bytes) in &mut index_iter {
            let handle = BlockHandle::decode(&handle_bytes).ctx()?;
            let block_data = self.read_block_cached(&handle).ctx()?;
            let block = Block::new(block_data).ctx()?;
            let mut iter = block.iter();
            for entry in &mut iter {
                result.push(entry);
            }
            if let Some(e) = iter.error() {
                return Err(e.clone()).ctx();
            }
        }
        if let Some(e) = index_iter.error() {
            return Err(e.clone()).ctx();
        }

        Ok(result)
    }

    /// Find the data block handle that might contain the given key.
    #[cfg(test)]
    fn find_data_block(&self, key: &[u8]) -> Result<Option<BlockHandle>> {
        match self.index_block.seek(key) {
            Some((_idx_key, handle_bytes)) => {
                let handle = BlockHandle::decode(&handle_bytes).ctx()?;
                Ok(Some(handle))
            }
            None => Ok(None),
        }
    }

    fn read_block_data(file: &mut File, handle: &BlockHandle) -> Result<Vec<u8>> {
        let file_size = file.metadata().ctx()?.len();
        Self::read_block_data_with_size(file, handle, file_size)
    }

    fn read_block_data_with_size(
        file: &mut File,
        handle: &BlockHandle,
        file_size: u64,
    ) -> Result<Vec<u8>> {
        const MAX_COMPRESSED_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
        let end = handle
            .offset
            .checked_add(handle.size)
            .and_then(|n| n.checked_add(BLOCK_TRAILER_SIZE as u64))
            .ok_or_else(|| Error::corruption("block handle range overflow"))
            .ctx()?;
        if end > file_size {
            return Err(Error::corruption(format!(
                "block handle out of bounds: offset={}, size={}, file_size={}",
                handle.offset, handle.size, file_size
            )));
        }
        if handle.size > MAX_COMPRESSED_BLOCK_SIZE {
            return Err(Error::corruption(format!(
                "compressed block size {} exceeds limit {}",
                handle.size, MAX_COMPRESSED_BLOCK_SIZE
            )));
        }
        file.seek(SeekFrom::Start(handle.offset)).ctx()?;
        let mut data = vec![0u8; handle.size as usize];
        file.read_exact(&mut data).ctx()?;

        // Read and verify trailer
        let mut trailer = [0u8; BLOCK_TRAILER_SIZE];
        file.read_exact(&mut trailer).ctx()?;

        let compression_type = CompressionType::from_u8(trailer[0])
            .ok_or_else(|| Error::corruption("unknown compression type"))
            .ctx()?;

        let stored_crc = u32::from_le_bytes(trailer[1..5].try_into().unwrap());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data);
        hasher.update(&[trailer[0]]);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(Error::corruption(format!(
                "block CRC mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            )));
        }

        // Decompress if needed (with size bound to prevent allocation bombs)
        let data = match compression_type {
            CompressionType::Lz4 => {
                if data.len() < 4 {
                    return Err(Error::corruption(
                        "LZ4 block too small for size header".to_string(),
                    ));
                }
                let uncompressed_size =
                    u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if uncompressed_size > MAX_DECOMPRESSED_BLOCK_SIZE {
                    return Err(Error::corruption(format!(
                        "LZ4 decompressed size {} exceeds limit {}",
                        uncompressed_size, MAX_DECOMPRESSED_BLOCK_SIZE
                    )));
                }
                lz4_flex::decompress_size_prepended(&data)
                    .map_err(|e| Error::corruption(format!("LZ4 decompression error: {}", e)))
                    .ctx()?
            }
            CompressionType::Zstd => {
                // zstd::bulk::compress() embeds the frame's uncompressed content
                // size by default (confirmed by the zstd crate's own test suite).
                // Read it via the stable zstd_safe API to right-size the
                // allocation, instead of always allocating the full
                // MAX_DECOMPRESSED_BLOCK_SIZE regardless of actual block size
                // (the alternative, `Decompressor::upper_bound()`, requires the
                // unstable "experimental" cargo feature, which is not enabled).
                // The claimed size is still bounded against
                // MAX_DECOMPRESSED_BLOCK_SIZE before use, exactly like the LZ4
                // path above, since a corrupted/adversarial frame header could
                // otherwise claim an oversized value.
                let capacity = match zstd::zstd_safe::get_frame_content_size(&data) {
                    Ok(Some(size)) => {
                        let size = size as usize;
                        if size > MAX_DECOMPRESSED_BLOCK_SIZE {
                            return Err(Error::corruption(format!(
                                "Zstd decompressed size {} exceeds limit {}",
                                size, MAX_DECOMPRESSED_BLOCK_SIZE
                            )));
                        }
                        size
                    }
                    // Frame doesn't embed a content size — fall back to the
                    // conservative bound (matches prior behavior for this case).
                    Ok(None) => MAX_DECOMPRESSED_BLOCK_SIZE,
                    Err(_) => {
                        return Err(Error::corruption("malformed Zstd frame header"));
                    }
                };
                zstd::bulk::decompress(data.as_slice(), capacity)
                    .map_err(|e| Error::corruption(format!("Zstd decompression error: {}", e)))
                    .ctx()?
            }
            CompressionType::None => data,
        };

        Ok(data)
    }

    fn read_metaindex(
        file: &mut File,
        metaindex_handle: &BlockHandle,
        file_size: u64,
    ) -> Result<MetaIndexData> {
        if metaindex_handle.size == 0 {
            return Ok(MetaIndexData {
                bloom: None,
                prefix: None,
                prefix_len: None,
                range_del_handle: None,
            });
        }

        let metaindex_data =
            Self::read_block_data_with_size(file, metaindex_handle, file_size).ctx()?;
        let metaindex = Block::from_vec(metaindex_data).ctx()?;

        let mut bloom = None;
        let mut prefix = None;
        let mut prefix_len = None;
        let mut range_del_handle = None;

        let mut iter = metaindex.iter();
        for (key, value) in &mut iter {
            if key == b"filter.bloom" {
                let handle = BlockHandle::decode(&value).ctx()?;
                bloom = Some(Self::read_block_data_with_size(file, &handle, file_size).ctx()?);
            } else if key == b"filter.prefix" {
                let handle = BlockHandle::decode(&value).ctx()?;
                prefix = Some(Self::read_block_data_with_size(file, &handle, file_size).ctx()?);
            } else if key == PREFIX_FILTER_LEN_NAME.as_bytes() {
                if value.len() != 8 {
                    return Err(Error::corruption(
                        "bad prefix filter length metadata".to_string(),
                    ));
                }
                let len = u64::from_le_bytes(value.as_slice().try_into().unwrap());
                prefix_len = Some(usize::try_from(len).map_err(|_| {
                    Error::corruption("prefix filter length overflows usize".to_string())
                })?);
            } else if key == RANGE_DEL_BLOCK_NAME.as_bytes() {
                range_del_handle = Some(BlockHandle::decode(&value).ctx()?);
            }
        }
        // Metaindex keys sort `filter.*` before `rangedelblock`: a corrupt
        // entry silently ending the scan early would drop `range_del_handle`,
        // and the legacy data-block fallback would then cache an *empty*
        // tombstone list for a new-format file — resurrecting deleted data.
        if let Some(e) = iter.error() {
            return Err(e.clone()).ctx();
        }
        Self::validate_prefix_filter_metadata(prefix.as_deref(), prefix_len).ctx()?;

        Ok(MetaIndexData {
            bloom,
            prefix,
            prefix_len,
            range_del_handle,
        })
    }

    /// Check if a prefix may exist in this SST file using the prefix bloom filter.
    /// Returns `true` if the prefix might be present (or if no prefix bloom exists).
    pub fn prefix_may_match(&self, prefix: &[u8]) -> bool {
        match (self.prefix_filter_data.as_ref(), self.prefix_filter_len) {
            (Some(filter), Some(prefix_len)) if prefix_len > 0 && prefix.len() >= prefix_len => {
                BloomFilter::key_may_match(&prefix[..prefix_len], filter)
            }
            _ => true, // Missing/incompatible metadata — conservatively assume present.
        }
    }

    /// Read a block, consulting the cache first if available.
    /// Returns Arc<Vec<u8>> — zero-copy on cache hit.
    fn read_block_cached(&self, handle: &BlockHandle) -> Result<Arc<Vec<u8>>> {
        self.read_block_cached_opt(handle, true)
    }

    /// Like [`read_block_cached`](Self::read_block_cached), but when
    /// `fill_cache` is false a cache miss does **not** insert the block into
    /// the block cache. Used by scans (user iterators with
    /// `ReadOptions::fill_cache == false`, and compaction reads) to avoid
    /// evicting hot point-read blocks.
    fn read_block_cached_opt(
        &self,
        handle: &BlockHandle,
        fill_cache: bool,
    ) -> Result<Arc<Vec<u8>>> {
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

        let mut file = self.open_file().ctx()?;
        let data = Self::read_block_data(&mut file, handle).ctx()?;

        if fill_cache && let Some(ref cache) = self.block_cache {
            return Ok(cache.insert(self.file_number, handle.offset, data));
        }

        Ok(Arc::new(data))
    }

    /// Pin this file's first data block in cache as non-evictable (for L0 files).
    /// Pre-populates the index entry cache and pins the first data block
    /// so that init_heap's first peek() always hits cache.
    pub fn pin_metadata_in_cache(&self) {
        // Populate the OnceLock index entry cache eagerly
        let entries = match self.cached_index_entries() {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("pin_metadata_in_cache: index decode error: {}", e);
                return;
            }
        };
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
            let (Ok(offset), Ok(len)) = (i64::try_from(offset), i64::try_from(len)) else {
                return;
            };
            if let Ok(file) = self.open_file() {
                // SAFETY: `posix_fadvise` is an advisory hint with no safety
                // invariants beyond a valid fd. The fd is obtained from a live
                // `File` via `AsRawFd` and remains valid for the `MutexGuard`
                // lifetime. Checked conversions keep offset and length
                // non-negative in the platform `off_t` representation.
                unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_WILLNEED);
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
        Ok(self.file.lock())
    }
}

#[cfg(test)]
mod tests {
    use super::TableReader;

    #[test]
    fn test_zero_prefix_filter_length_is_rejected() {
        assert!(
            TableReader::validate_prefix_filter_metadata(Some(&[1, 2]), Some(0)).is_err(),
            "a present filter cannot use the disabled zero-length prefix"
        );
        assert!(TableReader::validate_prefix_filter_metadata(Some(&[1, 2]), Some(3)).is_ok());
        assert!(
            TableReader::validate_prefix_filter_metadata(Some(&[1, 2]), None).is_ok(),
            "missing legacy metadata remains a conservative filter bypass"
        );
    }
}
