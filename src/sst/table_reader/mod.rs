//! SST table reader: reads key-value pairs from an SST file.
mod iterator;
pub use iterator::TableIterator;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, MutexGuard};

use crate::cache::block_cache::BlockCache;
use crate::error::{Error, Result};
use crate::iterator::range_del::FragmentedRangeTombstoneList;
use crate::sst::block::Block;
use crate::sst::filter::BloomFilter;
use crate::sst::format::{
    BLOCK_TRAILER_SIZE, BlockHandle, CompressionType, FOOTER_SIZE, PREFIX_FILTER_LEN_NAME,
    RANGE_DEL_BLOCK_NAME, decode_footer, decode_index_value_with_props,
};
use crate::stats::DbStats;
use crate::types::{InternalKeyRef, SequenceNumber, ValueType, compare_internal_key};
use ruc::*;

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
    path: PathBuf,
    file_size: u64,
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
        let index_data =
            Self::read_block_data_with_size(&mut file, &index_handle, file_size).c(d!())?;
        let index_block = Block::from_vec(index_data).c(d!())?;

        // Read filters and range-del handle from metaindex
        let meta = Self::read_metaindex(&mut file, &metaindex_handle, file_size).c(d!())?;

        Ok(Self {
            path: path.to_path_buf(),
            file_size,
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
        })
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
        index_block
            .iter()
            .map(|(k, v)| {
                let d = decode_index_value_with_props(&v).c(d!())?;
                Ok(IndexEntry {
                    separator_key: k,
                    handle: d.handle,
                    first_key: d.first_key.map(|fk| fk.to_vec()),
                    properties: d
                        .properties
                        .into_iter()
                        .map(|(n, p)| (n.to_vec(), p.to_vec()))
                        .collect(),
                })
            })
            .collect()
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

        // Use index block seek to find the data block that may contain our key.
        let handle = match self
            .index_block
            .seek_by(seek_key.as_bytes(), compare_internal_key)
        {
            Some((_idx_key, handle_bytes)) => BlockHandle::decode(&handle_bytes).c(d!())?,
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

        let handle = match self
            .index_block
            .seek_by(seek_key.as_bytes(), compare_internal_key)
        {
            Some((_idx_key, handle_bytes)) => BlockHandle::decode(&handle_bytes).c(d!())?,
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
    pub fn get_range_tombstones(&self) -> Result<Vec<RangeTombstoneEntry>> {
        let cached = self.cached_range_tombstones().c(d!())?;
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
                let handle = BlockHandle::decode(&handle_bytes).c(d!())?;
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

        let cached = Arc::new(FragmentedRangeTombstoneList::new(triples));
        // Race is benign — worst case we build twice
        let _ = self.range_tombstone_cache.set(cached.clone());
        Ok(cached)
    }

    /// Iterate over all key-value pairs in the table.
    pub fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = Vec::new();

        for (_, handle_bytes) in self.index_block.iter() {
            let handle = BlockHandle::decode(&handle_bytes).c(d!())?;
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
                let handle = BlockHandle::decode(&handle_bytes).c(d!())?;
                Ok(Some(handle))
            }
            None => Ok(None),
        }
    }

    fn read_block_data(file: &mut File, handle: &BlockHandle) -> Result<Vec<u8>> {
        let file_size = file.metadata().c(d!())?.len();
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
            .ok_or_else(|| Error::Corruption("block handle range overflow".to_string()))
            .c(d!())?;
        if end > file_size {
            return Err(eg!(Error::Corruption(format!(
                "block handle out of bounds: offset={}, size={}, file_size={}",
                handle.offset, handle.size, file_size
            ))));
        }
        if handle.size > MAX_COMPRESSED_BLOCK_SIZE {
            return Err(eg!(Error::Corruption(format!(
                "compressed block size {} exceeds limit {}",
                handle.size, MAX_COMPRESSED_BLOCK_SIZE
            ))));
        }
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

        // Decompress if needed (with size bound to prevent allocation bombs)
        let data = match compression_type {
            CompressionType::Lz4 => {
                if data.len() < 4 {
                    return Err(eg!(Error::Corruption(
                        "LZ4 block too small for size header".to_string()
                    )));
                }
                let uncompressed_size =
                    u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if uncompressed_size > MAX_DECOMPRESSED_BLOCK_SIZE {
                    return Err(eg!(Error::Corruption(format!(
                        "LZ4 decompressed size {} exceeds limit {}",
                        uncompressed_size, MAX_DECOMPRESSED_BLOCK_SIZE
                    ))));
                }
                lz4_flex::decompress_size_prepended(&data)
                    .map_err(|e| Error::Corruption(format!("LZ4 decompression error: {}", e)))
                    .c(d!())?
            }
            CompressionType::Zstd => {
                zstd::bulk::decompress(data.as_slice(), MAX_DECOMPRESSED_BLOCK_SIZE)
                    .map_err(|e| Error::Corruption(format!("Zstd decompression error: {}", e)))
                    .c(d!())?
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
            Self::read_block_data_with_size(file, metaindex_handle, file_size).c(d!())?;
        let metaindex = Block::from_vec(metaindex_data).c(d!())?;

        let mut bloom = None;
        let mut prefix = None;
        let mut prefix_len = None;
        let mut range_del_handle = None;

        for (key, value) in metaindex.iter() {
            if key == b"filter.bloom" {
                let handle = BlockHandle::decode(&value).c(d!())?;
                bloom = Some(Self::read_block_data_with_size(file, &handle, file_size).c(d!())?);
            } else if key == b"filter.prefix" {
                let handle = BlockHandle::decode(&value).c(d!())?;
                prefix = Some(Self::read_block_data_with_size(file, &handle, file_size).c(d!())?);
            } else if key == PREFIX_FILTER_LEN_NAME.as_bytes() {
                if value.len() != 8 {
                    return Err(eg!(Error::Corruption(
                        "bad prefix filter length metadata".to_string()
                    )));
                }
                let len = u64::from_le_bytes(value.as_slice().try_into().unwrap());
                prefix_len = Some(usize::try_from(len).map_err(|_| {
                    eg!(Error::Corruption(
                        "prefix filter length overflows usize".to_string()
                    ))
                })?);
            } else if key == RANGE_DEL_BLOCK_NAME.as_bytes() {
                range_del_handle = Some(BlockHandle::decode(&value).c(d!())?);
            }
        }

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
            (Some(filter), Some(prefix_len)) if prefix.len() >= prefix_len => {
                BloomFilter::key_may_match(&prefix[..prefix_len], filter)
            }
            _ => true, // Missing/incompatible metadata — conservatively assume present.
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
            if let Ok(file) = self.open_file() {
                // SAFETY: `posix_fadvise` is an advisory hint with no safety
                // invariants beyond a valid fd. The fd is obtained from a live
                // `File` via `AsRawFd` and remains valid for the `MutexGuard`
                // lifetime. The offset and length are derived from SST block
                // handles and cannot be negative.
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
        Ok(self.file.lock())
    }
}
